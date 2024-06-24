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

    /**
     * OBtiene  los datos de un arhivo de texto y los guarda en una sola Cadena de Texto.
     *
     * @param nodos Los nodos
     * @return Retorna una lista de tupla.
     */
    public ArrayList<Tupla> generarBufferMaps(int nodos) {
        System.out.println("Examinando archivo...");
        ArrayList<Tupla> listTupla = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        try {
            File file = new File(inputFile);
            if (file.exists()) {
                try (BufferedReader input = new BufferedReader(new FileReader(inputFile))) {
                    String line;
                    while ((line = input.readLine()) != null) {
                        sb.append(line.trim()).append(" ");
                    }
                } finally {
                    int hash = sb.hashCode() % nodos;
                    listTupla.add(new Tupla(hash, sb));
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return listTupla;
    }

    /**
     * Guarda el resultado en un arhivo de texto
     *
     * @param resultado Lista de tuplas con el resultado.
     */
    public void guardar(ArrayList<Tupla> resultado) {
        try {
            File file = new File(this.outputfile);
            if (file.exists()) {
                file.delete();
            }
            FileWriter myWriter = new FileWriter(this.outputfile);
            for (Tupla tupla : resultado) {
                myWriter.write(tupla.getClave() + " " + tupla.getValor() + "\n");
            }
            myWriter.close();
            System.out.println("Archivo creado exitosamente.");
        } catch (IOException e) {
            System.out.println("Ocurrió un error al escribir el archivo.");
            e.printStackTrace();
        }
    }
}
