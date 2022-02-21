package test;

public class c {
    public static void main(String[] args) {
        int[] a = {11, 41, 22, 35, 66, 27};
//        for (int i = 0; i < a.length; i++) {
//            for (int j = 0; j < i; j++) {
//                if (a[j] > a[j + 1]) {
//                    int tmp = a[j];
//                    a[j] = a[j + 1];
//                    a[j + 1] = tmp;
//                }
//            }
//        }
//        for (int i : a) {
//            StringBuffer stringBuffer = new StringBuffer();
//            stringBuffer.append(i);
//            stringBuffer.append(",");
//            stringBuffer.subSequence(0,stringBuffer.length()-1);
//            System.out.print(stringBuffer);
//        }

        for (int i = 0; i < a.length - 1; i++) {
            for (int j = 0; j < a.length - 1 - i; j++) {
                if (a[j] > a[j + 1]) {
                    int tmp = a[j];
                    a[j] = a[j + 1];
                    a[j + 1] = tmp;
                }
            }
        }

        for (int i : a) {
            System.out.print(i + ",");
        }

    }
}
