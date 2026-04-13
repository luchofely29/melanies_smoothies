package com.company;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

public class GodClass {
    public String userName = null;
    public String password = "admin123"; // credencial hardcodeada
    public Connection conn;
    public int counter = 0;
    public static int staticCounter = 0;
    public String lastResult = null;
    public Random random = new Random();
    public String filePath = null;
    public String sql = null;
    public GodClass() {
        // ...
    }
    // Método demasiado largo y complejo
    public void processEverything(String input, int x, int y) {
        // Código duplicado
        for (int i = 0; i < 2; i++) {
            if (input != null && input.length() > 0) {
                System.out.println("Processing: " + input);
            } else {
                System.out.println("No input");
            }
        }
        // Variables poco descriptivas
        int a = 0;
        int b = 1;
        int c = a + b;
        // Código muerto
        if (false) {
            System.out.println("Nunca se ejecuta");
        }
        // Uso de valores mágicos
        if (x == 42) {
            System.out.println("La respuesta secreta");
        }
        // Control de flujo confuso
        if (y > 10) {
            if (y < 20) {
                if (y % 2 == 0) {
                    System.out.println("Par y entre 10 y 20");
                } else {
                    if (y == 15) {
                        System.out.println("Es 15");
                    }
                }
            }
        }
        // Uso inseguro de random
        int token = random.nextInt();
        System.out.println("Token: " + token);
        // SQL Injection
        sql = "SELECT * FROM users WHERE name = '" + input + "'";
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db", userName, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println(rs.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace(); // Exposición de detalles
        }
        // Manejo inseguro de archivos
        try {
            FileInputStream fis = new FileInputStream(new File(filePath));
            int data = fis.read();
            while(data != -1) {
                System.out.print((char) data);
                data = fis.read();
            }
            fis.close();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        // Uso abusivo de null
        lastResult = null;
        // Excepción genérica
        try {
            int z = x / y;
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
        // Código duplicado
        for (int i = 0; i < 2; i++) {
            if (input != null && input.length() > 0) {
                System.out.println("Processing: " + input);
            } else {
                System.out.println("No input");
            }
        }
    }
    // Método estático mal utilizado
    public static void doStaticStuff() {
        staticCounter++;
        System.out.println("Static stuff: " + staticCounter);
    }
    // Falta de encapsulamiento
    public void setPassword(String pwd) {
        password = pwd;
    }
    // Comentario desactualizado
    // Este método borra todo (pero no lo hace)
    public void deleteAll() {
        System.out.println("No implementado");
    }
}
