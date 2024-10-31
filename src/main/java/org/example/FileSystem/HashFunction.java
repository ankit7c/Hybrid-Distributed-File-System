package org.example.FileSystem;

public class HashFunction {

    public static int hash(String input) {
        int asciiSum = 0;
        for (int i = 0; i < input.length(); i++) {
            asciiSum += input.charAt(i);  // Cast char to int to get ASCII value
        }
        return asciiSum % 16;
    }

    public static int hashCo(String input) {
        int asciiSum = 0;
        for (int i = 0; i < input.length(); i++) {
            asciiSum += input.charAt(i);  // Cast char to int to get ASCII value
        }
        return (asciiSum % 16 + 8) % 16;
    }

    public static void main(String[] args) {
        System.out.println(hash("Hello World"));
        System.out.println(hash("fa24-cs425-a601.cs.illinois.edu"));
        System.out.println(hash("fa24-cs425-a602.cs.illinois.edu"));
        System.out.println(hash("fa24-cs425-a603.cs.illinois.edu"));
        System.out.println(hash("fa24-cs425-a604.cs.illinois.edu"));
    }
}
