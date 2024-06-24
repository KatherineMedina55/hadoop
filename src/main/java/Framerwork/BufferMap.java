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
    private final ArrayList<Tupla> lstParticionada = new ArrayList<>();
    private final ArrayList<BufferReducer> lstOrdenada = new ArrayList<>();

    public BufferMap() {
    }

    public ArrayList<BufferReducer> getLstOrdenada() {
        return lstOrdenada;
    }

    public void particionarBuffer(ArrayList<Tupla> lstTuplas, int nodos) {
        for (Tupla tupla : lstTuplas) {
            int nodoReducer = tupla.getClave().hashCode() % nodos;
            lstParticionada.add(new Tupla(nodoReducer, tupla));
        }
    }
    /**
     * Ordena las tuplas segun su clave y valor.
     */
    public void ordenadoBuufer() {
        for (Tupla tupla : lstParticionada) {
            int clave1 = (int) tupla.getClave();

            int posicion = buscarbufferreduer(clave1);
            Tupla tuplaDelaTupla = (Tupla) tupla.getValor();

            if (posicion != -1) { //Se cumple esta condicion si encuentra la posicion de la tupla..
                BufferReducer bfr = lstOrdenada.get(posicion);
                bfr.agregarTuplaAlstTupla(tuplaDelaTupla);
                lstOrdenada.set(posicion, bfr);
            } else { // Caso contrario lo agrega como una nueva.
                ArrayList tmpLstValores = new ArrayList();
                tmpLstValores.add(tuplaDelaTupla.getValor());
                ArrayList<Tupla> tmp = new ArrayList<>();
                tmp.add(new Tupla(tuplaDelaTupla.getClave(), tmpLstValores));
                lstOrdenada.add(new BufferReducer(clave1, tmp));
            }
        }
    }

    /**
     * Buscar al objeto BufferReducer en la lista ordenada.
     *
     * @param reducer REducer que se quiere buscar
     * @return Retorna la posicion del reducer que se encontró.
     */
    public int buscarbufferreduer(int reducer) {
        for (int i = 0; i < lstOrdenada.size(); i++) {
            BufferReducer bufferReducer = lstOrdenada.get(i);
            if (bufferReducer.getNumReducer() == reducer) {
                return i;
            }
        }
        return -1;
    }

    public ArrayList<Tupla> getLstParticionada() {
        return lstParticionada;
    }
    
        public ArrayList<BufferReducer> getLstOrdered() {
        return lstOrdenada;
    }

}
