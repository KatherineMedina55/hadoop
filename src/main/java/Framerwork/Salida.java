
package Framerwork;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
/**
 *
 * @author andjo
 */
public class Salida {
       private String ArchivoSalida;

    public Salida(String outputfile) {
        this.ArchivoSalida = "./data/ejercicios/" + outputfile;
    }

    public void guardarArchivo(ArrayList<ParClaveValor> resultados) {
        try {
            File archivo = new File(ArchivoSalida);
            if (archivo.exists()) {
                archivo.delete();
            } else {
                System.out.println("Archivo no existe, se creara uno nuevo.");
            }
            try (FileWriter escritor = new FileWriter(ArchivoSalida)) {
                for (ParClaveValor parClaveValor : resultados) {
                    escritor.write(parClaveValor.getClave() + " " + parClaveValor.getValor() + "\n");
                }
            }
            System.out.println("Archivo guardado exitosamente.");
        } catch (IOException e) {
            System.out.println("Error al escribir el archivo.");
            e.printStackTrace();
        }
    } 
}
