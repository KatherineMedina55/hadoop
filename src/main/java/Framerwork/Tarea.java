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
    private int node;

    private MyMap mapFunction;
    private MapReduce reduceFunction;
    private Object combinerFunction;
     public Tarea() {
        super();
        inputFile = "";
        outputfile = "";
        node = 0;
        mapFunction = null;
        reduceFunction = null;
        combinerFunction = "";
    }
      public void run() {
        ManejoArchivo arhivoM = new ManejoArchivo(this.inputFile, this.outputfile);

        BufferMap bfm = new BufferMap();
        ArrayList<Tupla> resultado = new ArrayList<>();
        ArrayList<Tupla> lstBuffers = arhivoM.generarBufferMaps(node);
     if (lstBuffers.isEmpty()){
            Logger.getAnonymousLogger().severe("No se ha podido cargar el archivo");
            return;
        }
        System.out.println("Iniciando proceso de Map");
        for (Tupla tupla : lstBuffers) {
            ArrayList<Tupla> output = new ArrayList<>();
            mapFunction.Map(tupla, output);
            bfm.particionarBuffer(output, this.node);
        }
        System.out.println("Iniciando proceso de Ordenamiento");
        bfm.ordenadoBuufer();
        ArrayList<BufferReducer> lstOrdenada = bfm.getLstOrdenada();
        System.out.println("Iniciando proceso de Reduce");
        for (BufferReducer bufferReducer : lstOrdenada) {
            ArrayList<Tupla> lstTuplasReducer = bufferReducer.getLstTuplas();
            for (Tupla tuplaReducer : lstTuplasReducer) {
                reduceFunction.reduce(tuplaReducer, resultado);
            }
        }
        System.out.println("Guardando los datos en " + outputfile);
        arhivoM.guardar(resultado);
    }

    public Tarea(String inputFile, String outputfile, int node, MyMap mapFunction, MapReduce reduceFunction, Object combinerFunction) {
        this.inputFile = inputFile;
        this.outputfile = outputfile;
        this.node = node;
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
        return node;
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
        this.node = node;
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
