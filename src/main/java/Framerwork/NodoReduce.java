/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Framerwork;

import java.util.ArrayList;
import java.util.List;
/**
 *
 * @author andjo
 */
public class NodoReduce {
     private int numeroReducidores;
    private ArrayList<Tupla> listaTuplas;

 public NodoReduce(int numeroReducidores, ArrayList<Tupla> listaTuplas) {
        this.numeroReducidores = numeroReducidores;
        this.listaTuplas = listaTuplas;
    }

  
    public int buscarTuplaEnLst(Tupla tupla) {
        for (int i = 0; i < listaTuplas.size(); i++) {
            String claveTpTmp = ((String) (listaTuplas.get(i)).getClave());
            if (claveTpTmp.compareTo((String) tupla.getClave()) == 0) {
                return i;
            }
        }
        return -1;
    }

     public void agregarTuplaAlstTupla(Tupla nuevaTupla) {
        int indice = buscarTuplaEnLst(nuevaTupla);
        if (indice != -1) {
            Tupla tuplaExistente = listaTuplas.get(indice);
            ArrayList<Object> valores = (ArrayList<Object>) tuplaExistente.getValor();
            valores.add(nuevaTupla.getValor());
            listaTuplas.set(indice, new Tupla(nuevaTupla.getClave(), valores));
        } else {
            ArrayList<Object> valores = new ArrayList<>();
            valores.add(nuevaTupla.getValor());
            listaTuplas.add(new Tupla(nuevaTupla.getClave(), valores));
        }
    }

    public int getNumReducer() {
        return numeroReducidores;
    }

    public ArrayList<Tupla> getLstTuplas() {
        return listaTuplas;
    } 
}
