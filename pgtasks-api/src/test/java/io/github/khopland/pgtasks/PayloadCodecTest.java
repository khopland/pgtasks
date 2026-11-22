package io.github.khopland.pgtasks;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadCodecTest {

    private record Point(int x, int y) {

        byte[] toBytes() {
            return new byte[] {(byte) x, (byte) y, (byte) 0xff, 0};
        }

        static Point parse(byte[] bytes) {
            return new Point(bytes[0], bytes[1]);
        }
    }

    @Test
    void binary_payloads_are_stored_as_base64_text() {
        PayloadCodec<Point> codec = PayloadCodec.binary(Point::toBytes, Point::parse);

        String stored = codec.encode(new Point(3, 4));

        assertThat(stored).isEqualTo("AwT/AA==");
        assertThat(codec.decode(stored)).isEqualTo(new Point(3, 4));
        assertThat(codec.type()).isNull();
    }

    @Test
    void bytes_round_trip_whatever_they_hold() {
        byte[] bytes = {0, 1, (byte) 0x80, (byte) 0xff, '\n', '\''};

        assertThat(PayloadCodec.bytes().decode(PayloadCodec.bytes().encode(bytes))).containsExactly(bytes);
        assertThat(PayloadCodec.bytes().encode("hi".getBytes(StandardCharsets.UTF_8))).isEqualTo("aGk=");
    }

    @Test
    void text_that_isnt_base64_fails_to_decode() {
        assertThatThrownBy(() -> PayloadCodec.bytes().decode("not base64!")).isInstanceOf(IllegalArgumentException.class);
    }
}
