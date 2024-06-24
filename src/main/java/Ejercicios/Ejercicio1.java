/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Ejercicios;
import Framerwork.BufferMap;
import Framerwork.MyMap;
import Framerwork.MapReduce;
import Framerwork.Tarea;
import Framerwork.Tupla;
import java.util.ArrayList;

/**
 *
 * @author andjo
 */
public class Ejercicio1  {

    
    
     static class Map implements MyMap {
        @Override
        public void Map(Tupla elemento, ArrayList<Tupla> output) {
            String[] words = ((elemento.getValor())).toString().split(" "); //
            for (String w : words) {
                // Remove punctuation and convert to lowercase
                String new_w = w.toLowerCase();
                new_w = new_w.replaceAll("[^\\w]", ""); // El regex significa que reemplace en vacio las palabras que no sean numero o palabras.
                if (new_w.equals("404"))
                    output.add(new Tupla(new_w, 1));
            }
     }
}
          
     static class Reduce1 implements MapReduce {

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
        Tarea t = new Tarea();
        t.setInputFile("weblog.txt");
        t.setOutputfile("Ejercicio1.txt");
        t.setNode(52);
        t.setMapFunction(new Map());
        t.setReduceFunction(new Reduce1());
        t.run();
    }  
}