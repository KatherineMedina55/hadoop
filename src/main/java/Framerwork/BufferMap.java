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
public class BufferMap {
        private final ArrayList<Tupla> listaParticionada = new ArrayList<>();
    private final ArrayList<BufferReducer> listaOrdenada = new ArrayList<>();


    public BufferMap() {
    }

    public ArrayList<BufferReducer> getLstOrdenada() {
        return listaOrdenada;
    }


     public void particionarBuffer(ArrayList<Tupla> listaTuplas, int numReducers) {
        for (Tupla tupla : listaTuplas) {
            int indice = tupla.getClave().hashCode() % numReducers;
            listaParticionada.add(new Tupla(indice, tupla));
        }
    }
   
       public void ordenarBuffer() {
        for (Tupla tupla : listaParticionada) {
            int clave = (int) tupla.getClave();
            int posicion = buscarbufferreduer(clave);
            Tupla valorTupla = (Tupla) tupla.getValor();

            if (posicion != -1) { // Si se encuentra la posición de la tupla
                BufferReducer bufferReducer = listaOrdenada.get(posicion);
                bufferReducer.agregarTuplaAlstTupla(valorTupla);
                listaOrdenada.set(posicion, bufferReducer);
            } else { // Si no se encuentra, se agrega como una nueva entrada
                ArrayList<Object> valoresTemporales = new ArrayList<>();
                valoresTemporales.add(valorTupla.getValor());
                ArrayList<Tupla> nuevaListaTuplas = new ArrayList<>();
                nuevaListaTuplas.add(new Tupla(valorTupla.getClave(), valoresTemporales));
                listaOrdenada.add(new BufferReducer(clave, nuevaListaTuplas));
            }
        }
    }



    public int buscarbufferreduer(int reducer) {
        for (int i = 0; i < listaOrdenada.size(); i++) {
            BufferReducer bufferReducer = listaOrdenada.get(i);
            if (bufferReducer.getNumReducer()== reducer) {
                return i;
            }
        }
        return -1;
    }

     public ArrayList<Tupla> getListaParticionada() {
        return listaParticionada;
    }
    
  public ArrayList<BufferReducer> getListaOrdenada() {
        return listaOrdenada;
    }

}
