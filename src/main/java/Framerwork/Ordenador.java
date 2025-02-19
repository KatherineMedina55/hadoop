
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

    public void ordenarBuffer(ArrayList<ParClaveValor> listaParticionada) {
        for (ParClaveValor particion : listaParticionada) {
            int clave = (int) particion.getClave();
            int posicion = buscarBufferReducer(clave);
            ParClaveValor valorParClaveValor = (ParClaveValor) particion.getValor();

            if (posicion != -1) { // Si se encuentra la posición de la tupla
                NodoReduce bufferReducer = listaOrdenada.get(posicion);
                bufferReducer.agregarTuplaAlstTupla(valorParClaveValor);
                listaOrdenada.set(posicion, bufferReducer);
            } else { // Si no se encuentra, se agrega como una nueva entrada
                ArrayList<Object> valoresTemporales = new ArrayList<>();
                valoresTemporales.add(valorParClaveValor.getValor());
                ArrayList<ParClaveValor> nuevaListaTuplas = new ArrayList<>();
                nuevaListaTuplas.add(new ParClaveValor(valorParClaveValor.getClave(), valoresTemporales));
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
