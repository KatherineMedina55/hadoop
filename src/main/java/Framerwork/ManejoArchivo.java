/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Framerwork;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author andjo
 */
public class ManejoArchivo {
       String inputFile;
    String outputfile;

    public ManejoArchivo(String inputFile, String output) {
        super();
        this.inputFile = "./data/" + inputFile;
        this.outputfile = "./data/ejercicios/" + output;
    }


 public ArrayList<Tupla> crearBufferMaps(int numeroNodos) {
        System.out.println("Examinando archivo...");
        ArrayList<Tupla> listaTuplas = new ArrayList<>();
        StringBuilder contenidoArchivo = new StringBuilder();
        try {
            File archivo = new File(inputFile);
            if (archivo.exists()) {
                try (BufferedReader lector = new BufferedReader(new FileReader(inputFile))) {
                    String linea;
                    while ((linea = lector.readLine()) != null) {
                        contenidoArchivo.append(linea.trim()).append(" ");
                    }
                } finally {
                    int hash = contenidoArchivo.toString().hashCode() % numeroNodos;
                    listaTuplas.add(new Tupla(hash, contenidoArchivo.toString()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listaTuplas;
    }


    public void guardarArchivo(ArrayList<Tupla> resultados) {
        try {
            File archivo = new File(outputfile);
            if (archivo.exists()) {
                archivo.delete();
            }
            try (FileWriter escritor = new FileWriter(outputfile)) {
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

