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
public class Ejercicio2 {
     static class map1 implements NodoMap  {
        /**
         * Guarda en la lista output los ".gif" con un valor 1
         *
         * @param elemento Tupla
         * @param output   ArrayList que permite agregar las tuplas que queremos.
         */
        @Override
        public void Map(Tupla elemento, ArrayList<Tupla> output) {
            String[] words = ((elemento.getValor())).toString().split(" ");
            for (String w : words) {
                // Remove punctuation and convert to lowercase
                String new_w = w.toLowerCase();
                if (new_w.contains(".gif"))// Compara si cada texto existe la ".gif2
                    output.add(new Tupla("gif", 1));
            }
        }
    }

    static class reduce implements MapReduce {
        /**
         * Cuenta cuantas 404 en la lista de tuplas ordenadas.
         *
         * @param elemento Tupla
         * @param output   ArrayList que permite agregar las tuplas que queremo, en la cual será el resultado.
         */
        @Override
        public void reduce(Tupla elemento, ArrayList<Tupla> output) {
            ArrayList<Integer> list = (ArrayList<Integer>) elemento.getValor();
            int count = 0;
            for (Integer item : list) {
                count += item;
            }
            output.add(new Tupla(elemento.getClave(), count));
        }
    }

    public static void main(String[] args) {
        Tarea tarea1 = new Tarea();
        tarea1.setReduceFunction(new reduce());
        tarea1.setMapFunction(new map1());
        tarea1.setInputFile("weblog.txt");
        tarea1.setOutputfile("Ejercicio2.txt");
        tarea1.setNode(2);
        tarea1.run();
    }
}
