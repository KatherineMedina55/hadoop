
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


    public void particionarBuffer(ArrayList<ParClaveValor> listaParClaveValor, int numReducers) {
        for (ParClaveValor parClaveValor : listaParClaveValor) {
            int nodoReducer = parClaveValor.getClave().hashCode() % numReducers;
            listaParticionada.add(new ParClaveValor(nodoReducer, parClaveValor));
        }
    }


     public ArrayList<ParClaveValor> getListaParticionada() {
        return listaParticionada;
    }
    


}
