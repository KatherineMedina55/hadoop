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
        private final ArrayList<ParClaveValor> listaParticionada = new ArrayList<>();

    public Particionador() {
    }


    public void particionarBuffer(ArrayList<ParClaveValor> listaTuplas, int numReducers) {
        for (ParClaveValor tupla : listaTuplas) {
            int nodoReducer = tupla.getClave().hashCode() % numReducers;
            listaParticionada.add(new ParClaveValor(nodoReducer, tupla));
        }
    }


     public ArrayList<ParClaveValor> getListaParticionada() {
        return listaParticionada;
    }
    


}
