
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
    private ArrayList<ParClaveValor> listaParClaveValor;

 public NodoReduce(int numeroReducidores, ArrayList<ParClaveValor> listaTuplas) {
        this.numeroReducidores = numeroReducidores;
        this.listaParClaveValor = listaTuplas;
    }

  
    public int buscarTuplaEnLst(ParClaveValor tupla) {
        for (int i = 0; i < listaParClaveValor.size(); i++) {
            String claveTpTmp = ((String) (listaParClaveValor.get(i)).getClave());
            if (claveTpTmp.compareTo((String) tupla.getClave()) == 0) {
                return i;
            }
        }
        return -1;
    }

     public void agregarTuplaAlstTupla(ParClaveValor nuevaTupla) {
        int indice = buscarTuplaEnLst(nuevaTupla);
        if (indice != -1) {
            ParClaveValor tuplaExistente = listaParClaveValor.get(indice);
            ArrayList<Object> valores = (ArrayList<Object>) tuplaExistente.getValor();
            valores.add(nuevaTupla.getValor());
            listaParClaveValor.set(indice, new ParClaveValor(nuevaTupla.getClave(), valores));
        } else {
            ArrayList<Object> valores = new ArrayList<>();
            valores.add(nuevaTupla.getValor());
            listaParClaveValor.add(new ParClaveValor(nuevaTupla.getClave(), valores));
        }
    }
      public void ejecutarReduce(BiFunction<Object, List<Object>, Object> reduceFunction) {
        ArrayList<ParClaveValor> resultados = new ArrayList<>();
        for (ParClaveValor tupla : listaParClaveValor) {
            String clave = (String) tupla.getClave();
            List<Object> valores = (List<Object>) tupla.getValor();
            Object resultado = reduceFunction.apply(clave, valores);
            resultados.add(new ParClaveValor(clave, resultado));
        }
        listaParClaveValor = resultados;
    }

    public int getNumReducer() {
        return numeroReducidores;
    }

    public ArrayList<ParClaveValor> getLstTuplas() {
        return listaParClaveValor;
    } 
}
