/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class X
{
    public static void main(String[] args)
            throws CharacterCodingException
    {
        boolean first = true;
        CharBuffer lastUtf16 = null;
        Slice lastUtf8 = null;
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
        for (int codepoint = 0; codepoint <= 0x10FFFF; codepoint++) {
            if (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
                continue;
            }
            CharBuffer utf16 = CharBuffer.wrap(Character.toChars(codepoint));
            Slice utf8 = null;
            try {
                utf8 = Slices.wrappedBuffer(encoder.encode(utf16.duplicate()));
            }
            catch (CharacterCodingException e) {
                System.err.println(String.format("Error encoding codepoint %X", codepoint));
                e.printStackTrace();
                throw e;
            }

            if (first) {
                first = false;
                lastUtf16 = utf16.duplicate();
                lastUtf8 = utf8;
            }
            else {
                Preconditions.checkState(utf16.duplicate().compareTo(lastUtf16.duplicate()) > 0, "broken utf16 comparison for codepoint: %s", codepoint);
                Preconditions.checkState(utf8.compareTo(lastUtf8) > 0, "broken utf8 comparison for codepoint: %s", codepoint);
            }
        }
    }
}
