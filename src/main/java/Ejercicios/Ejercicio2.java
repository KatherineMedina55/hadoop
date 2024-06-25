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
     static class GifMap  implements NodoMap  {
        /**
         * Guarda en la lista output los ".gif" con un valor 1
         *
         * @param tuplaEntrada Tupla
         * @param listaSalida   ArrayList que permite agregar las tuplas que queremos.
         */
        @Override
        public void Map(Tupla tuplaEntrada, ArrayList<Tupla> listaSalida) {
            String[] palabras  = ((tuplaEntrada.getValor())).toString().split(" ");
            for (String palabra  : palabras ) {
                // Remove punctuation and convert to lowercase
                String palabraLimpia  = palabra.toLowerCase();
                if (palabraLimpia.contains(".gif"))// Compara si cada texto existe la ".gif2
                    listaSalida.add(new Tupla("gif", 1));
            }
        }
    }

    static class SumReduce  implements MapReduce {
       /**
         * Cuenta cuántos ".gif" hay en la lista de tuplas ordenadas.
         *
         * @param tuplaClaveValores Tupla que contiene la clave y la lista de valores asociados.
         * @param listaSalida   ArrayList que permite agregar las tuplas que queremos, en la cual será el resultado.
         */
        @Override
        public void reduce(Tupla tuplaClaveValores, ArrayList<Tupla> listaSalida) {
            ArrayList<Integer> listaValores = (ArrayList<Integer>) tuplaClaveValores.getValor();
            int totalGif = listaValores.stream().mapToInt(Integer::intValue).sum();
            listaSalida.add(new Tupla(tuplaClaveValores.getClave(), totalGif));
        }
    }

    public static void main(String[] args) {
        Tarea tarea1 = new Tarea();
        tarea1.setReduceFunction(new SumReduce ());
        tarea1.setMapFunction(new GifMap ());
        tarea1.setInputFile("weblog.txt");
        tarea1.setOutputfile("Ejercicio2.txt");
        tarea1.setNode(2);
        tarea1.run();
    }
}
