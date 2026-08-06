package info.dvkr.screenstream.vnc.internal

import android.graphics.Bitmap
import com.elvishew.xlog.XLog
import info.dvkr.screenstream.common.getLog
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readFully
import io.ktor.utils.io.readInt
import io.ktor.utils.io.readShort
import io.ktor.utils.io.writeByte
import io.ktor.utils.io.writeFully
import io.ktor.utils.io.writeInt
import io.ktor.utils.io.writeShort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.zip.Deflater

internal class RfbConnection(
    private val parentScope: CoroutineScope,
    private val socket: Socket,
    private val address: String,
    private val bitmapStateFlow: StateFlow<Bitmap>,
    private val maxFps: () -> Int,
    private val zlibEnabled: () -> Boolean,
    private val onClosed: () -> Unit
) {
    private companion object {
        private const val RFB_VERSION = "RFB 003.008\n"
        private const val SERVER_NAME = "ScreenStream VNC"
        private const val ENCODING_RAW = 0
        private const val ENCODING_ZRLE = 16
        private const val ZRLE_TILE_SIZE = 64
        private const val MAX_CUT_TEXT_SKIP = 1 shl 20

        private val VERSION_REGEX = Regex("RFB\\s\\d{3}\\.\\d{3}")

        private val PIXEL_FORMAT: ByteArray = byteArrayOf(
            32,          // Bits per pixel
            24,          // Depth
            0,           // Big endian flag
            1,           // True colour flag
            0x00, 0xFF.toByte(),  // Red max
            0x00, 0xFF.toByte(),  // Green max
            0x00, 0xFF.toByte(),  // Blue max
            16,          // Red shift
            8,           // Green shift
            0,           // Blue shift
            0, 0, 0      // Padding
        )
    }

    private val scope = CoroutineScope(parentScope.coroutineContext + SupervisorJob())
    private var runJob: Job? = null

    @Volatile
    private var clientEncodings: Set<Int> = emptySet()

    private var zrleDeflater: Deflater? = null

    internal fun start() {
        runJob = scope.launch { run() }
    }

    internal suspend fun awaitTermination() {
        runJob?.join()
    }

    private suspend fun run() {
        val input = socket.openReadChannel()
        val output = socket.openWriteChannel()
        try {
            handshake(input, output)
            clientInit(input)
            val (width, height) = serverInit(output)
            XLog.i(getLog("RfbConnection", "Client connected: $address, ${width}x$height"))
            scope.launch { frameLoop(output, width, height) }
            clientLoop(input)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (throwable: Throwable) {
            XLog.d(getLog("RfbConnection", "Client $address disconnected: ${throwable.message}"))
        } finally {
            close()
        }
    }

    private suspend fun handshake(input: ByteReadChannel, output: ByteWriteChannel) {
        output.writeFully(RFB_VERSION.toByteArray(Charsets.US_ASCII))
        output.flush()

        val clientVersion = ByteArray(12)
        input.readFully(clientVersion)
        val versionString = String(clientVersion, Charsets.US_ASCII).trim()
        if (!VERSION_REGEX.matches(versionString)) {
            throw IOException("Unsupported client version '$versionString'")
        }
        XLog.d(getLog("RfbConnection.handshake", "Client version: $versionString"))

        output.writeByte(0x01.toByte()) // Number of security types
        output.writeByte(0x01.toByte()) // Security type: None
        output.flush()

        val chosenType = input.readByte().toInt() and 0xFF
        if (chosenType != 0x01) {
            sendSecurityFailure(output, "Unsupported security type $chosenType")
            throw IOException("Client chose unsupported security type $chosenType")
        }

        output.writeInt(0) // Security result: OK
        output.flush()
    }

    private suspend fun sendSecurityFailure(output: ByteWriteChannel, reason: String) {
        val reasonBytes = reason.toByteArray(Charsets.UTF_8)
        output.writeInt(1) // Security result: FAIL
        output.writeInt(reasonBytes.size)
        output.writeFully(reasonBytes)
        output.flush()
    }

    private suspend fun clientInit(input: ByteReadChannel) {
        input.readByte() // Shared flag (ignored, we are view-only)
    }

    private suspend fun serverInit(output: ByteWriteChannel): Pair<Int, Int> {
        val bitmap = bitmapStateFlow.value
        val width = bitmap.width.toShort()
        val height = bitmap.height.toShort()

        output.writeShort(width)
        output.writeShort(height)
        output.writeFully(PIXEL_FORMAT)
        val name = SERVER_NAME.toByteArray(Charsets.UTF_8)
        output.writeInt(name.size)
        output.writeFully(name)
        output.flush()

        return bitmap.width to bitmap.height
    }

    private suspend fun clientLoop(input: ByteReadChannel) {
        while (scope.isActive) {
            when (val type = input.readByte().toInt() and 0xFF) {
                0x00 -> { // SetPixelFormat (ignored)
                    input.readFully(ByteArray(3))
                    input.readFully(ByteArray(16))
                }

                0x02 -> { // SetEncodings
                    input.readByte() // Padding
                    val count = input.readShort().toInt() and 0xFFFF
                    val encodings = HashSet<Int>(count)
                    repeat(count) { encodings.add(input.readInt()) }
                    clientEncodings = encodings
                }

                0x03 -> { // FramebufferUpdateRequest (view-only: ignored)
                    input.readByte()
                    input.readShort()
                    input.readShort()
                    input.readShort()
                    input.readShort()
                }

                0x04 -> { // KeyEvent (ignored, view-only)
                    input.readByte() // Down flag
                    input.readByte() // Padding
                    input.readByte() // Padding
                    input.readInt()  // Key
                }

                0x05 -> { // PointerEvent (ignored, view-only)
                    input.readByte()
                    input.readShort()
                    input.readShort()
                }

                0x06 -> { // ClientCutText (ignored)
                    input.readFully(ByteArray(3)) // Padding
                    val length = input.readInt().toLong() and 0xFFFFFFFFL
                    val skip = length.coerceAtMost(MAX_CUT_TEXT_SKIP.toLong()).toInt()
                    input.readFully(ByteArray(skip))
                    if (skip.toLong() < length) throw IOException("ClientCutText too large ($length bytes)")
                }

                else -> throw IOException("Unknown message type 0x${type.toString(16)} from VNC client")
            }
        }
    }

    private suspend fun frameLoop(output: ByteWriteChannel, width: Int, height: Int) {
        var lastBitmap: Bitmap? = null
        while (scope.isActive) {
            val bitmap = bitmapStateFlow.value
            if (bitmap !== lastBitmap) {
                lastBitmap = bitmap
                if (bitmap.width != width || bitmap.height != height) {
                    throw IOException("Framebuffer resized to ${bitmap.width}x${bitmap.height}, closing connection")
                }
                sendFrameUpdate(output, bitmap, width, height)
            }
            val fps = maxFps().coerceIn(1, 60)
            delay(1000L / fps)
        }
    }

    private suspend fun sendFrameUpdate(output: ByteWriteChannel, bitmap: Bitmap, width: Int, height: Int) {
        val pixels = IntArray(width * height)
        bitmap.getPixels(pixels, 0, width, 0, 0, width, height)

        val useZRLE = zlibEnabled() && ENCODING_ZRLE in clientEncodings
        val tileBytes = if (useZRLE) buildZRLE(pixels, width, height) else null

        output.writeByte(0x00.toByte()) // FramebufferUpdate
        output.writeByte(0x00.toByte()) // Padding
        output.writeShort(1.toShort())   // Number of rectangles
        output.writeShort(0.toShort())   // X
        output.writeShort(0.toShort())   // Y
        output.writeShort(width.toShort())
        output.writeShort(height.toShort())
        if (tileBytes != null) {
            output.writeInt(ENCODING_ZRLE)
            val compressed = flushZRLE(tileBytes)
            output.writeInt(compressed.size)
            output.writeFully(compressed)
        } else {
            output.writeInt(ENCODING_RAW)
            for (pixel in pixels) {
                output.writeByte((pixel and 0xFF).toByte())       // Blue
                output.writeByte(((pixel ushr 8) and 0xFF).toByte()) // Green
                output.writeByte(((pixel ushr 16) and 0xFF).toByte()) // Red
                output.writeByte(0x00.toByte())
            }
        }
        output.flush()
    }

    private fun buildZRLE(pixels: IntArray, width: Int, height: Int): ByteArray {
        val tile = ZRLE_TILE_SIZE
        val rows = (height + tile - 1) / tile
        val cols = (width + tile - 1) / tile
        val zrle = ByteArray(rows * cols * (1 + tile * tile * 3))
        var offset = 0
        var ty = 0
        while (ty < height) {
            val th = minOf(tile, height - ty)
            var tx = 0
            while (tx < width) {
                val tw = minOf(tile, width - tx)
                zrle[offset++] = 0x00.toByte() // Subencoding: raw pixels
                for (row in 0 until th) {
                    var index = (ty + row) * width + tx
                    for (col in 0 until tw) {
                        val pixel = pixels[index + col]
                        zrle[offset++] = (pixel and 0xFF).toByte()       // Blue
                        zrle[offset++] = ((pixel ushr 8) and 0xFF).toByte() // Green
                        zrle[offset++] = ((pixel ushr 16) and 0xFF).toByte() // Red
                    }
                }
                tx += tile
            }
            ty += tile
        }
        return zrle.copyOf(offset)
    }

    private fun flushZRLE(tileBytes: ByteArray): ByteArray {
        val deflater = zrleDeflater ?: Deflater(Deflater.DEFAULT_COMPRESSION).also { zrleDeflater = it }
        val out = ByteArrayOutputStream(16384)
        val buffer = ByteArray(65536)
        deflater.setInput(tileBytes)
        while (!deflater.needsInput()) {
            val size = deflater.deflate(buffer, 0, buffer.size, Deflater.NO_FLUSH)
            if (size == 0) break
            out.write(buffer, 0, size)
        }
        while (true) {
            val size = deflater.deflate(buffer, 0, buffer.size, Deflater.SYNC_FLUSH)
            if (size == 0) break
            out.write(buffer, 0, size)
        }
        return out.toByteArray()
    }

    private fun close() {
        runCatching { socket.close() }
        runCatching { zrleDeflater?.end() }
        zrleDeflater = null
        scope.cancel()
        onClosed()
    }
}
