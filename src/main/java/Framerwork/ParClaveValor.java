
package Framerwork;

/**
 *
 * @author andjo
 */
public class ParClaveValor {
    
    private Object clave;
    private Object valor;

    public ParClaveValor(Object clave, Object valor) {
        this.clave = clave;
        this.valor = valor;
    }

    public Object getClave() {
        return clave;
    }

    public Object getValor() {
        return valor;
    }

    public void setClave(Object clave) {
        this.clave = clave;
    }

    public void setValor(Object valor) {
        this.valor = valor;
    }
}
