package com.company;

import java.util.Date;
import java.util.Scanner;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.FileInputStream;
import java.io.Serializable;

public class Utils implements Serializable {
    public static String VERSION = "1.0";
    public static String lastUser = null;
    public static int calc_sum(int a, int b) {
        // Estilo inconsistente y nombres incorrectos
        int resultado_final = a+b;
        return resultado_final;
    }
    // Método muerto
    public static void unusedMethod() {
        // ...
    }
    // Deserialización insegura
    public static Object insecureDeserialize(String path) throws Exception {
        FileInputStream fis = new FileInputStream(path);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object obj = ois.readObject();
        ois.close();
        return obj;
    }
    // Comparación insegura
    public static boolean insecureCompare(String a, String b) {
        return a == b;
    }
    // API obsoleta
    public static void printDate() {
        Date d = new Date(2020, 1, 1);
        System.out.println(d.toLocaleString());
    }
    // Falta de validación de entrada
    public static void writeToFile(String fileName, String content) throws Exception {
        FileOutputStream fos = new FileOutputStream(fileName);
        fos.write(content.getBytes());
        fos.close();
    }
    // Falta de control de acceso
    public static void adminAction() {
        System.out.println("Acción de admin ejecutada");
    }
    // Uso abusivo de null
    public static String getNull() {
        return null;
    }
    // Uso de credenciales hardcodeadas
    public static String getApiKey() {
        return "apikey-123456";
    }
}
