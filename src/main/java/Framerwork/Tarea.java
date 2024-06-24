/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Framerwork;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author andjo
 */
public class Tarea {
    private String inputFile;
    private String outputfile;
    private int numNodos;

    private MyMap mapFunction;
    private MapReduce reduceFunction;
    private Object combinerFunction;
     public Tarea() {
        super();
        inputFile = "";
        outputfile = "";
        numNodos = 0;
        mapFunction = null;
        reduceFunction = null;
        combinerFunction = "";
    }
      public void run() {
        EntradaSalidaArchivos gestionArchivo  = new EntradaSalidaArchivos(this.inputFile, this.outputfile);

        Particionador bfm = new Particionador();
        ArrayList<Tupla> resultado = new ArrayList<>();
        ArrayList<Tupla> Listabuffer  = gestionArchivo.crearBufferMaps(numNodos);
     if (Listabuffer.isEmpty()){
            Logger.getAnonymousLogger().severe("No se ha podido cargar el archivo");
            return;
        }
        System.out.println("Iniciando proceso de Map");
        for (Tupla tupla : Listabuffer) {
            ArrayList<Tupla> output = new ArrayList<>();
            mapFunction.Map(tupla, output);
            bfm.particionarBuffer(output, this.numNodos);
        }
        System.out.println("Iniciando proceso de Ordenamiento");
        bfm.ordenarBuffer();
        ArrayList<NodoReduce> listaOrdenada = bfm.obtenerListaOrdenada();
       
        System.out.println("Iniciando proceso de Reduce");
        for (NodoReduce bufferReducer : listaOrdenada) {
            ArrayList<Tupla> listaTuplasReducer  = bufferReducer.getLstTuplas();
            for (Tupla tuplaReducer : listaTuplasReducer ) {
                reduceFunction.reduce(tuplaReducer, resultado);
            }
        }
        System.out.println("Guardando los datos en " + outputfile);
        gestionArchivo.guardarArchivo(resultado);
    }

    public Tarea(String inputFile, String outputfile, int node, MyMap mapFunction, MapReduce reduceFunction, Object combinerFunction) {
        this.inputFile = inputFile;
        this.outputfile = outputfile;
        this.numNodos = node;
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.combinerFunction = combinerFunction;
    }

    public String getInputFile() {
        return inputFile;
    }

    public String getOutputfile() {
        return outputfile;
    }

    public int getNode() {
        return numNodos;
    }

    public MyMap getMapFunction() {
        return mapFunction;
    }

    public MapReduce getReduceFunction() {
        return reduceFunction;
    }

    public Object getCombinerFunction() {
        return combinerFunction;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public void setOutputfile(String outputfile) {
        this.outputfile = outputfile;
    }

    public void setNode(int node) {
        this.numNodos = node;
    }

    public void setMapFunction(MyMap mapFunction) {
        this.mapFunction = mapFunction;
    }

    public void setReduceFunction(MapReduce reduceFunction) {
        this.reduceFunction = reduceFunction;
    }

    public void setCombinerFunction(Object combinerFunction) {
        this.combinerFunction = combinerFunction;
    }
    
    
}
