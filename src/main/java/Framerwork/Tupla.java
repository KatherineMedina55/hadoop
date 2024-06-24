/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Framerwork;

/**
 *
 * @author andjo
 */
public class Tupla {
    
    private Object clave;
    private Object valor;

    public Tupla(Object clave, Object valor) {
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
