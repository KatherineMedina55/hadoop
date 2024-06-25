/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Ejercicios;

import Framerwork.MapReduce;
import Framerwork.NodoMap;
import Framerwork.Tarea;
import Framerwork.ParClaveValor;
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
            public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                String[] line = elemento.getValor().toString().split(" ");
                for (String item : line) {
                    String[] lineData = item.split(",");
                    if (!lineData[8].equals(lineData[12])) {
                        output.add(new ParClaveValor(item, 1));
                    }
                }
            }
        });
        tarea1.setReduceFunction(new MapReduce() {
            @Override
            public void reduce(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
                output.add(new ParClaveValor(elemento.getClave(), ""));
            }
        });
        tarea1.setInputFile("JCMB_last31days.csv");
        tarea1.setOutputfile("Ejercicio4.txt");
        tarea1.setNode(30);
        tarea1.run();
    }
}
