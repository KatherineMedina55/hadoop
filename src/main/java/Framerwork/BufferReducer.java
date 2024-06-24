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
public class BufferReducer {
       private int numReducer;
    private ArrayList<Tupla> lstTuplas;

    public BufferReducer(int numReducer, ArrayList<Tupla> lstTuplas) {
        super();
        this.numReducer = numReducer;
        this.lstTuplas = lstTuplas;
    }

    /**
     * Devuelve la posicion de la tupla de la lista.
     * @param tupla Tupla que se quiere encontrar.
     * @return Posición de la tupla que será devuelta, en caso de no encontrar la tupla retorna -1.
     */
    public int buscarTuplaEnLst(Tupla tupla) {
        for (int i = 0; i < lstTuplas.size(); i++) {
            String claveTpTmp = ((String) (lstTuplas.get(i)).getClave());
            if (claveTpTmp.compareTo((String) tupla.getClave()) == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Agrega un tupla en la lista de tuplas.
     *
     * @param tp Tupla que será agregada.
     */
    public void agregarTuplaAlstTupla(Tupla tp) {
        int index = buscarTuplaEnLst(tp);
        if (index != -1) {
            Tupla tptmp = lstTuplas.get(index);
            ArrayList lastTmp = (ArrayList) tptmp.getValor();
            lastTmp.add(tp.getValor());
            lstTuplas.set(index, new Tupla(tp.getClave(), lastTmp));
        } else {
            ArrayList lstTmp = new ArrayList();
            lstTmp.add(tp.getValor());
            lstTuplas.add(new Tupla(tp.getClave(), lstTmp));
        }
    }

    public int getNumReducer() {
        return numReducer;
    }

    public ArrayList<Tupla> getLstTuplas() {
        return lstTuplas;
    } 
}
