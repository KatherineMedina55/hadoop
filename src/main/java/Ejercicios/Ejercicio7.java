/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Ejercicios;



import Framerwork.Particionador;
import Framerwork.MapReduce;
import Framerwork.Tarea;
import Framerwork.Tupla;
import java.util.ArrayList;
import Framerwork.NodoMap;
/**
 *
 * @author andjo
 */
public class Ejercicio7 {
    public static void main(String[] args) {
        Tarea tarea = new Tarea();
        tarea.setMapFunction(new NodoMap() {

            @Override
            public void Map(Tupla elemento, ArrayList<Tupla> output) {
                String[] line = elemento.getValor().toString().split(" ");
                for (String item : line) {
                    String[] lineData = item.split("\\t");
                    double happiness_average = Double.parseDouble(lineData[2]);
                    if (happiness_average < 2 && !lineData[4].equals("--")) {
                        output.add(new Tupla("palabras_extremadamente_triste", 1));
                    }
                }
            }
        });
        tarea.setReduceFunction(new MapReduce() {

            @Override
            public void reduce(Tupla elemento, ArrayList<Tupla> output) {
                ArrayList<Integer> list = (ArrayList<Integer>) elemento.getValor();
                int count = 0;
                for (Integer item : list) {
                    count += item;
                }
                output.add(new Tupla(elemento.getClave(), count));
            }
        });
        tarea.setInputFile("happiness.txt");
        tarea.setOutputfile("Ejercicio7.txt");
        tarea.setNode(30);
        tarea.run();
    }
}
