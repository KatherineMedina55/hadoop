/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Framerwork;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
/**
 *
 * @author andjo
 */
public class NodoReduce {
     private int numeroReducidores;
    private ArrayList<ParClaveValor> listaTuplas;

 public NodoReduce(int numeroReducidores, ArrayList<ParClaveValor> listaTuplas) {
        this.numeroReducidores = numeroReducidores;
        this.listaTuplas = listaTuplas;
    }

  
    public int buscarTuplaEnLst(ParClaveValor tupla) {
        for (int i = 0; i < listaTuplas.size(); i++) {
            String claveTpTmp = ((String) (listaTuplas.get(i)).getClave());
            if (claveTpTmp.compareTo((String) tupla.getClave()) == 0) {
                return i;
            }
        }
        return -1;
    }

     public void agregarTuplaAlstTupla(ParClaveValor nuevaTupla) {
        int indice = buscarTuplaEnLst(nuevaTupla);
        if (indice != -1) {
            ParClaveValor tuplaExistente = listaTuplas.get(indice);
            ArrayList<Object> valores = (ArrayList<Object>) tuplaExistente.getValor();
            valores.add(nuevaTupla.getValor());
            listaTuplas.set(indice, new ParClaveValor(nuevaTupla.getClave(), valores));
        } else {
            ArrayList<Object> valores = new ArrayList<>();
            valores.add(nuevaTupla.getValor());
            listaTuplas.add(new ParClaveValor(nuevaTupla.getClave(), valores));
        }
    }
      public void ejecutarReduce(BiFunction<Object, List<Object>, Object> reduceFunction) {
        ArrayList<ParClaveValor> resultados = new ArrayList<>();
        for (ParClaveValor tupla : listaTuplas) {
            String clave = (String) tupla.getClave();
            List<Object> valores = (List<Object>) tupla.getValor();
            Object resultado = reduceFunction.apply(clave, valores);
            resultados.add(new ParClaveValor(clave, resultado));
        }
        listaTuplas = resultados;
    }

    public int getNumReducer() {
        return numeroReducidores;
    }

    public ArrayList<ParClaveValor> getLstTuplas() {
        return listaTuplas;
    } 
}
