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
public class Ejercicio3 {
 
    
     static class map1 implements NodoMap {
        /**
         * Guarda en la lista las horas.
         * @param elemento ParClaveValor
         * @param output  .
         */
        @Override
        public void Map(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
            String[] words = ((elemento.getValor())).toString().split(" ");
            for (String wordss : words) {
                if (wordss.startsWith("[") && wordss.endsWith("]")) {
                    String hora = wordss.split(":")[1];
                    output.add(new ParClaveValor(hora, 1));
                }
            }
        }
    }

    static class reduce1 implements MapReduce {
        /**
         * Cuenta las horas
         *
         * @param elemento ParClaveValor
         * @param output   .
         */
        @Override
        public void reduce(ParClaveValor elemento, ArrayList<ParClaveValor> output) {
            ArrayList<Integer> list = (ArrayList<Integer>) elemento.getValor();
            int count = 0;
            for (Integer item : list) {
                count += item;
            }

            output.add(new ParClaveValor(elemento.getClave(), count));
        }
    }


    public static void main(String[] args) {
        Tarea t = new Tarea();
        t.setInputFile("weblog.txt");
        t.setOutputfile("Ejercicio3.txt");
        t.setNode(52);
        t.setMapFunction(new map1());
        t.setReduceFunction(new reduce1());
        t.run();
    }
    
    
    
    
    
    
}
