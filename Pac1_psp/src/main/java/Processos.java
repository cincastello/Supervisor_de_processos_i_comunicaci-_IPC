import java.io.IOException;

public class Processos {
    ProcessBuilder pb = new ProcessBuilder("java", "-version");
    String termeCerca= "";
    String fitxer= "/home/alumnat/Baixades/Telegram Desktop/";

    ProcessBuilder pb2 = new ProcessBuilder(
        "grep",
        "--",
        termeCerca,
        fitxer.toString()
        );
    Process p = pb2.start();


    public Processos() throws IOException {
    }
}
