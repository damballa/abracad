/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package abracad.avro;

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;

public class ArrayAccessor {

    public static void writeArray(Object data, Encoder out) throws IOException {
        if (data instanceof boolean[]) {
            writeArray((boolean[]) data, out);
        } else if (data instanceof short[]) {
            writeArray((short[]) data, out);
        } else if (data instanceof char[]) {
            writeArray((char[]) data, out);
        } else if (data instanceof int[]) {
            writeArray((int[]) data, out);
        } else if (data instanceof long[]) {
            writeArray((long[]) data, out);
        } else if (data instanceof float[]) {
            writeArray((float[]) data, out);
        } else if (data instanceof double[]) {
            writeArray((double[]) data, out);
        }
    }

    public static void writeArray(boolean[] data, Encoder out) throws IOException {
        int size = data.length;
        out.setItemCount(size);
        for (int i = 0; i < size; i++) {
            out.startItem();
            out.writeBoolean(data[i]);
        }
    }

    // short, and char arrays are upcast to avro int
    public static void writeArray(short[] data, Encoder out) throws IOException {
        int size = data.length;
        out.setItemCount(size);
        for (int i = 0; i < size; i++) {
            out.startItem();
            out.writeInt(data[i]);
        }
    }

    public static void writeArray(char[] data, Encoder out) throws IOException {
        int size = data.length;
        out.setItemCount(size);
        for (int i = 0; i < size; i++) {
            out.startItem();
            out.writeInt(data[i]);
        }
    }

    public static void writeArray(int[] data, Encoder out) throws IOException {
        int size = data.length;
        out.setItemCount(size);
        for (int i = 0; i < size; i++) {
            out.startItem();
            out.writeInt(data[i]);
        }
    }

    public static void writeArray(long[] data, Encoder out) throws IOException {
        int size = data.length;
        out.setItemCount(size);
        for (int i = 0; i < size; i++) {
            out.startItem();
            out.writeLong(data[i]);
        }
    }

    public static void writeArray(float[] data, Encoder out) throws IOException {
        int size = data.length;
        out.setItemCount(size);
        for (int i = 0; i < size; i++) {
            out.startItem();
            out.writeFloat(data[i]);
        }
    }

    public static void writeArray(double[] data, Encoder out) throws IOException {
        int size = data.length;
        out.setItemCount(size);
        for (int i = 0; i < size; i++) {
            out.startItem();
            out.writeDouble(data[i]);
        }
    }

    public static Object readArray(Object array, Class<?> elementType, long l, ResolvingDecoder in)
            throws IOException {
        if (elementType == int.class)
            return readArray((int[]) array, l, in);
        if (elementType == long.class)
            return readArray((long[]) array, l, in);
        if (elementType == float.class)
            return readArray((float[]) array, l, in);
        if (elementType == double.class)
            return readArray((double[]) array, l, in);
        if (elementType == boolean.class)
            return readArray((boolean[]) array, l, in);
        if (elementType == char.class)
            return readArray((char[]) array, l, in);
        if (elementType == short.class)
            return readArray((short[]) array, l, in);
        return null;
    }

    public static boolean[] readArray(boolean[] array, long l, ResolvingDecoder in)
            throws IOException {
        int index = 0;
        while (l > 0) {
            int limit = index + (int) l;
            if (array.length < limit) {
                array = Arrays.copyOf(array, limit);
            }
            while (index < limit) {
                array[index] = in.readBoolean();
                index++;
            }
            l = in.arrayNext();
        }
        return array;
    }

    public static int[] readArray(int[] array, long l, ResolvingDecoder in)
            throws IOException {
        int index = 0;
        while (l > 0) {
            int limit = index + (int) l;
            if (array.length < limit) {
                array = Arrays.copyOf(array, limit);
            }
            while (index < limit) {
                array[index] = in.readInt();
                index++;
            }
            l = in.arrayNext();
        }
        return array;
    }

    public static short[] readArray(short[] array, long l, ResolvingDecoder in)
            throws IOException {
        int index = 0;
        while (l > 0) {
            int limit = index + (int) l;
            if (array.length < limit) {
                array = Arrays.copyOf(array, limit);
            }
            while (index < limit) {
                array[index] = (short) in.readInt();
                index++;
            }
            l = in.arrayNext();
        }
        return array;
    }

    public static char[] readArray(char[] array, long l, ResolvingDecoder in)
            throws IOException {
        int index = 0;
        while (l > 0) {
            int limit = index + (int) l;
            if (array.length < limit) {
                array = Arrays.copyOf(array, limit);
            }
            while (index < limit) {
                array[index] = (char) in.readInt();
                index++;
            }
            l = in.arrayNext();
        }
        return array;
    }

    public static long[] readArray(long[] array, long l, ResolvingDecoder in)
            throws IOException {
        int index = 0;
        while (l > 0) {
            int limit = index + (int) l;
            if (array.length < limit) {
                array = Arrays.copyOf(array, limit);
            }
            while (index < limit) {
                array[index] = in.readLong();
                index++;
            }
            l = in.arrayNext();
        }
        return array;
    }

    public static float[] readArray(float[] array, long l, ResolvingDecoder in)
            throws IOException {
        int index = 0;
        while (l > 0) {
            int limit = index + (int) l;
            if (array.length < limit) {
                array = Arrays.copyOf(array, limit);
            }
            while (index < limit) {
                array[index] = in.readFloat();
                index++;
            }
            l = in.arrayNext();
        }
        return array;
    }

    public static double[] readArray(double[] array, long l, ResolvingDecoder in)
            throws IOException {
        int index = 0;
        while (l > 0) {
            int limit = index + (int) l;
            if (array.length < limit) {
                array = Arrays.copyOf(array, limit);
            }
            while (index < limit) {
                array[index] = in.readDouble();
                index++;
            }
            l = in.arrayNext();
        }
        return array;
    }

}
