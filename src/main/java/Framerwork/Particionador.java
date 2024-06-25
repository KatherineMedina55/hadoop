/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Framerwork;

import java.util.ArrayList;

/**
 *
 * @author andjo
 */
public class Particionador {
        private final ArrayList<Tupla> listaParticionada = new ArrayList<>();

    public Particionador() {
    }


    public void particionarBuffer(ArrayList<Tupla> listaTuplas, int numReducers) {
        for (Tupla tupla : listaTuplas) {
            int nodoReducer = tupla.getClave().hashCode() % numReducers;
            listaParticionada.add(new Tupla(nodoReducer, tupla));
        }
    }


     public ArrayList<Tupla> getListaParticionada() {
        return listaParticionada;
    }
    


}
