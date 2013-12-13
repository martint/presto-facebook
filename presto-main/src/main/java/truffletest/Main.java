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
package truffletest;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.nodes.RootNode;

import java.util.concurrent.ThreadLocalRandom;

public class Main
{
    public static void main(String[] args)
    {
        //new LoopNode(AddNodeFactory.create(new ArgumentReferenceNode("a"), new ArgumentReferenceNode("b")))
        final RootNode expression = new EntryPointNode(
                new LoopNode(
                        AddNodeFactory.create(
                                IntegerLiteralNodeFactory.create(1),
                                IntegerLiteralNodeFactory.create(2))));

        CallTarget target = Truffle.getRuntime().createCallTarget(expression);

        Object[] arguments = new Object[1];
        int sum = 0;
        for (int i = 0; i < 1_000_000; i++) {
//            arguments[0] = i;//ThreadLocalRandom.current().nextInt();
            sum += (Integer) target.call();
//            sum += (Integer) target.call(new ArgumentsArray(arguments));
        }

        System.out.println(sum);

//        for (int i = 0; i < 5000000; i++) {
//            arguments.put("a", NullableInt.valueOf(ThreadLocalRandom.current().nextInt()));
//            arguments.put("b", ThreadLocalRandom.current().nextInt());
//            Object value = target.call(new NamedArguments(arguments));
//
//            if (value instanceof NullableInt && !((NullableInt) value).isNull()) {
//                sum += ((NullableInt) value).getValue();
//            }
//            else if (value instanceof Integer) {
//                sum += ((Integer) value).intValue();
//            }
//        }

//        System.out.println(sum);
    }
}
