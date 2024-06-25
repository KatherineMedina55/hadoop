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
public class Ordenador {
       private final ArrayList<NodoReduce> listaOrdenada = new ArrayList<>();

    public Ordenador() {
    }

    public ArrayList<NodoReduce> getListaOrdenada() {
        return listaOrdenada;
    }

    public void ordenarBuffer(ArrayList<Tupla> listaParticionada) {
        for (Tupla particion : listaParticionada) {
            int clave = (int) particion.getClave();
            int posicion = buscarBufferReducer(clave);
            Tupla valorTupla = (Tupla) particion.getValor();

            if (posicion != -1) { // Si se encuentra la posición de la tupla
                NodoReduce bufferReducer = listaOrdenada.get(posicion);
                bufferReducer.agregarTuplaAlstTupla(valorTupla);
                listaOrdenada.set(posicion, bufferReducer);
            } else { // Si no se encuentra, se agrega como una nueva entrada
                ArrayList<Object> valoresTemporales = new ArrayList<>();
                valoresTemporales.add(valorTupla.getValor());
                ArrayList<Tupla> nuevaListaTuplas = new ArrayList<>();
                nuevaListaTuplas.add(new Tupla(valorTupla.getClave(), valoresTemporales));
                listaOrdenada.add(new NodoReduce(clave, nuevaListaTuplas));
            }
        }
    }

    public int buscarBufferReducer(int reducer) {
        for (int i = 0; i < listaOrdenada.size(); i++) {
            NodoReduce bufferReducer = listaOrdenada.get(i);
            if (bufferReducer.getNumReducer() == reducer) {
                return i;
            }
        }
        return -1;
    }
}
