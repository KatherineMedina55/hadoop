/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.NodoMap;
import Framerwork.Tarea;
import Framerwork.Tupla;
import java.util.ArrayList;

/**
 *
 * @author andjo
 */
public class Ejercicio4 {
     public static void main(String[] args) {
        Tarea tarea1 = new Tarea();
        tarea1.setMapFunction(new NodoMap() {

            @Override
            public void Map(Tupla elemento, ArrayList<Tupla> output) {
                String[] line = elemento.getValor().toString().split(" ");
                for (String item : line) {
                    String[] lineData = item.split(",");
                    if (!lineData[8].equals(lineData[12])) {
                        output.add(new Tupla(item, 1));
                    }
                }
            }
        });
        tarea1.setReduceFunction(new MapReduce() {
            @Override
            public void reduce(Tupla elemento, ArrayList<Tupla> output) {
                output.add(new Tupla(elemento.getClave(), ""));
            }
        });
        tarea1.setInputFile("JCMB_last31days.csv");
        tarea1.setOutputfile("Ejercicio4.txt");
        tarea1.setNode(30);
        tarea1.run();
    }
}
