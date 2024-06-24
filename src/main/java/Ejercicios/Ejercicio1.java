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
            String[] palabras = elemento.getValor().toString().split(" ");
            for (String palabra : palabras) {
                String nuevaPalabra = palabra.toLowerCase().replaceAll("[^\\w]", "");
                if (nuevaPalabra.equals("404")) {
                    output.add(new Tupla(nuevaPalabra, 1));
                }
            }
     }
}
          
     static class Reduce implements MapReduce {

        @Override
        public void reduce(Tupla tupla, ArrayList<Tupla> output) {
            ArrayList<Integer> lista = (ArrayList<Integer>) tupla.getValor();
            int contador  = 0;
            for (Integer valor: lista) {
                contador  += valor;
            }
            output.add(new Tupla(tupla.getClave(), contador ));
        }
    }

     public static void main(String[] args) {
        Tarea t = new Tarea();
        t.setInputFile("weblog.txt");
        t.setOutputfile("Ejercicio1.txt");
        t.setNode(52);
        t.setMapFunction(new Map());
        t.setReduceFunction(new Reduce());
        t.run();
    }  
}