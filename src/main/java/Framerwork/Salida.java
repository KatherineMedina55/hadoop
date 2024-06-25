/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


package Framerwork;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
/**
 *
 * @author andjo
 */
public class Salida {
       private String ArchivoSalida;

    public Salida(String outputfile) {
        this.ArchivoSalida = "./data/ejercicios/" + outputfile;
    }

    public void guardarArchivo(ArrayList<Tupla> resultados) {
        try {
            File archivo = new File(ArchivoSalida);
            if (archivo.exists()) {
                archivo.delete();
            } else {
                System.out.println("Archivo no existe, se creara uno nuevo.");
            }
            try (FileWriter escritor = new FileWriter(ArchivoSalida)) {
                for (Tupla tupla : resultados) {
                    escritor.write(tupla.getClave() + " " + tupla.getValor() + "\n");
                }
            }
            System.out.println("Archivo guardado exitosamente.");
        } catch (IOException e) {
            System.out.println("Error al escribir el archivo.");
            e.printStackTrace();
        }
    } 
}
