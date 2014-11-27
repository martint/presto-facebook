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

import io.airlift.slice.Slices;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Y
{
    public static void main(String[] args)
    {
        byte[] low = new byte[] {(byte) 0xf0, (byte) 0x9d, (byte) 0x90, (byte) 0x83};
        byte[] high = new byte[] {(byte) 0xef, (byte) 0xbf, (byte) 0xbd};

        String lowString = new String(low, UTF_8);
        System.out.println("low: " + lowString);
        int lowCodePoint = Character.codePointAt(lowString.toCharArray(), 0);
        System.out.println(String.format("low codepoint: %X", lowCodePoint));
        System.out.println();

        String highString = new String(high, UTF_8);
        System.out.println("high: " + highString);
        int highCodePoint = Character.codePointAt(highString.toCharArray(), 0);
        System.out.println(String.format("high codepoint: %X", highCodePoint));
        System.out.println();

        System.out.println("byte-wise comparison: " + Slices.wrappedBuffer(low).compareTo(Slices.wrappedBuffer(high)));
        System.out.println("char-wise comparison: " + lowString.compareTo(highString));
        System.out.println("codepoint-wise comparison: " + Integer.compare(lowCodePoint, highCodePoint));


    }
}
