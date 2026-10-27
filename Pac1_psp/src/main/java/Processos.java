import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class Processos {

    private static final DateTimeFormatter HORA =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    record LogLine(Instant time, String source, String text) {
        String format() {
            return "[" + HORA.format(time) + "] " + source + " | " + text;
        }
    }

    record ProcessResult(List<String> command, long pid, long ppid, int exitCode,
                         boolean timedOut, String cause, Duration duration,
                         List<LogLine> stdout, List<LogLine> stderr) {
        ProcessResult {
            command = List.copyOf(command);
            stdout = List.copyOf(stdout);
            stderr = List.copyOf(stderr);
        }

        boolean ok() {
            return cause.equals("EXIT") && exitCode == 0;
        }
    }

    static final class Cancel {
        volatile String cause;

        void request(String why) {
            if (cause == null) {
                cause = "CANCEL (" + why + ")";
            }
        }
    }

    private static final Set<Process> ACTIVE = ConcurrentHashMap.newKeySet();

    /** Fa atòmica la creació d'un procés respecte al hook de tancament. */
    private static final Object START_LOCK = new Object();

    /** Es posa a true quan el hook de tancament comença. */
    private static volatile boolean INTERROMPUT;

    /** Nombre de descendents que tenia l'últim procés aturat (perquè les proves no siguin falsos positius). */
    private static volatile int DESCENDENTS_ATURATS;

    /** PPID observat de cada fill en el moment de crear-lo (després de morir ja no es pot consultar). */
    private static final Map<Long, Long> PPIDS = new ConcurrentHashMap<>();

    private static synchronized void say(String role, String message) {
        System.out.println(diag(role, message));
    }

    private static String diag(String role, String message) {
        ProcessHandle me = ProcessHandle.current();
        long ppid = me.parent().map(ProcessHandle::pid).orElse(-1L);
        return String.format("[%s pid=%d ppid=%d thread=%s] %s",
                role, me.pid(), ppid, Thread.currentThread().getName(), message);
    }

    public static void main(String[] args) throws Exception {
        String rol = args.length == 0
                ? "supervisor"
                : args[0].toLowerCase(Locale.ROOT);

        switch (rol) {
            case "supervisor" -> runSupervisor();
            case "worker" -> runWorker(args);
            case "producer" -> runProducer();
            case "consumer" -> runConsumer();
            case "echo" -> runEcho();
            default -> throw new IllegalArgumentException(
                    "Rol desconegut: " + rol
            );
        }
    }

    private static void runSupervisor() throws Exception {
        String scenario = System.getProperty("escenari", "demo");
        installShutdownHook();
        say("SUPERVISOR", "inici, escenari=" + scenario);
        switch (scenario.toLowerCase(Locale.ROOT)) {
            case "parallel" -> parallelDemo();
            case "stdin" -> stdinDemo();
            case "timeout" -> timeoutDemo();
            case "pipe" -> pipeDemo();
            case "flood" -> floodDemo();
            case "cancel" -> cancelDemo();
            case "arbre" -> arbreDemo();
            case "tests" -> {
                if (!runTests()) {
                    System.exit(1);
                }
            }
            case "demo" -> {
                parallelDemo();
                stdinDemo();
                timeoutDemo();
                pipeDemo();
            }
            default -> throw new IllegalArgumentException(
                    "Escenari desconegut: " + scenario + ". Useu: demo, parallel, stdin, "
                            + "timeout, pipe, flood, cancel, arbre o tests");
        }
        say("SUPERVISOR", "fi. Processos encara actius: " + ACTIVE.size());
    }

    private static void installShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            List<Process> actius;
            synchronized (START_LOCK) {
                INTERROMPUT = true;
                actius = List.copyOf(ACTIVE);
            }
            for (Process p : actius) {
                if (p.isAlive()) {
                    System.err.println("[HOOK] causa=INTERRUPCIO del supervisor, cancel·lant pid="
                            + p.pid());
                    try {
                        terminate("HOOK", p);
                    } catch (InterruptedException e) {
                        p.descendants().forEach(ProcessHandle::destroyForcibly);
                        p.destroyForcibly();
                    }
                }
            }
        }, "shutdown-hook"));
    }
    record Task(String name, int seconds, int exitCode) { }

    record ParallelOutcome(List<ProcessResult> results, int peak) { }

    private static ParallelOutcome parallelDemo() throws Exception {
        say("SUPERVISOR", "parallelDemo: 3 workers, màxim 2 alhora");
        List<Task> tasks = List.of(
                new Task("Alpha", 3, 0),
                new Task("Beta", 2, 7),
                new Task("Gamma", 2, 0));

        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(2, namedThreads("pool"));
        List<Future<ProcessResult>> futures = new ArrayList<>();
        List<ProcessResult> results = new ArrayList<>();
        try {
            for (Task t : tasks) {
                futures.add(pool.submit((Callable<ProcessResult>) () -> {
                    String label = "WORKER-" + t.name().toUpperCase(Locale.ROOT);
                    int now = running.incrementAndGet();
                    peak.accumulateAndGet(now, Math::max);
                    say("SUPERVISOR", "inicia " + label + " (concurrents ara: " + now + ")");
                    try {
                        return supervise(label, command("worker", t.name(),
                                String.valueOf(t.seconds()), String.valueOf(t.exitCode()),
                                "normal"), null, Duration.ofSeconds(20));
                    } finally {
                        running.decrementAndGet();
                        say("SUPERVISOR", "ha acabat " + label);
                    }
                }));
            }
            int ok = 0;
            int errors = 0;
            for (int i = 0; i < futures.size(); i++) {
                ProcessResult r = futures.get(i).get();
                results.add(r);
                report("WORKER-" + tasks.get(i).name().toUpperCase(Locale.ROOT), r);
                if (r.ok()) {
                    ok++;
                } else {
                    errors++;
                }
            }
            say("SUPERVISOR", "AGREGAT: " + ok + " correctes, " + errors
                    + " amb error, pic de concurrència=" + peak.get());
            printProcessTable(List.of("Worker Alpha", "Worker Beta", "Worker Gamma"), results);
        } finally {
            pool.shutdown();
        }
        return new ParallelOutcome(results, peak.get());
    }

    private static ProcessResult stdinDemo() throws Exception {
        say("SUPERVISOR", "stdinDemo: enviem text a stdin d'un fill");
        ProcessResult r = supervise("ECHO", command("echo"),
                "Ebre\nprocessos\nFI\n", Duration.ofSeconds(10));
        report("ECHO", r);
        return r;
    }

    private static ProcessResult timeoutDemo() throws Exception {
        say("SUPERVISOR", "timeoutDemo: worker de 30 s amb límit de 3 s");
        ProcessResult r = supervise("WORKER-LENT",
                command("worker", "Lent", "30", "0", "spawn"), null, Duration.ofSeconds(3));
        report("WORKER-LENT", r);

        // Verifiquem que no queda cap procés viu.
        say("SUPERVISOR", "worker pid=" + r.pid() + " viu? " + isAlive(r.pid()));
        OptionalLong net = netPid(r);
        net.ifPresent(pid -> say("SUPERVISOR", "nét pid=" + pid + " viu? " + isAlive(pid)));
        say("SUPERVISOR", "Comprova-ho també amb: ps -eo pid,ppid,stat,etime,cmd | grep Processo");
        return r;
    }

    private static ProcessResult[] pipeDemo() throws Exception {
        say("SUPERVISOR", "pipeDemo: producer -> supervisor -> consumer");
        Instant inici = Instant.now();
        List<String> cmdProducer = command("producer");
        List<String> cmdConsumer = command("consumer");
        Process producer = startProcess(cmdProducer);
        Process consumer = startProcess(cmdConsumer);

        FutureTask<List<LogLine>> prodErr = drain(producer.getErrorStream(), "stderr");
        FutureTask<List<LogLine>> consOut = drain(consumer.getInputStream(), "stdout");
        FutureTask<List<LogLine>> consErr = drain(consumer.getErrorStream(), "stderr");

        List<LogLine> passades = Collections.synchronizedList(new ArrayList<>());
        Thread bridge = new Thread(() -> {
            try (BufferedReader from = new BufferedReader(
                    new InputStreamReader(producer.getInputStream(), StandardCharsets.UTF_8));
                 BufferedWriter to = consumer.outputWriter(StandardCharsets.UTF_8)) {
                String line;
                while ((line = from.readLine()) != null) {
                    passades.add(new LogLine(Instant.now(), "stdout", line));
                    say("SUPERVISOR", "pont: '" + line + "' -> consumer");
                    to.write(line);
                    to.newLine();
                    to.flush();
                }
            } catch (IOException e) {
                say("SUPERVISOR", "error al pont: " + e.getMessage());
            }
        }, "bridge");
        bridge.start();

        boolean prodAcaba = producer.waitFor(15, TimeUnit.SECONDS);
        bridge.join(5000);
        boolean consAcaba = consumer.waitFor(15, TimeUnit.SECONDS);
        if (!prodAcaba) {
            terminate("PRODUCER", producer);
        }
        if (!consAcaba) {
            terminate("CONSUMER", consumer);
        }

        ProcessResult rp = buildResult(cmdProducer, producer, inici, prodAcaba ? "EXIT" : "TIMEOUT",
                passades, collect(prodErr));
        ProcessResult rc = buildResult(cmdConsumer, consumer, inici, consAcaba ? "EXIT" : "TIMEOUT",
                collect(consOut), collect(consErr));
        report("PRODUCER", rp);
        report("CONSUMER", rc);
        say("SUPERVISOR", "RESULTAT PIPE: codis producer=" + rp.exitCode()
                + " consumer=" + rc.exitCode());
        return new ProcessResult[]{rp, rc};
    }

    record Tree(long supervisor, long worker, long workerParent, long net, long netParent) { }

    /** Arbre de processos en viu amb l'API portable ProcessHandle (alternativa a pstree). */
    private static Tree arbreDemo() throws Exception {
        say("SUPERVISOR", "arbreDemo: arbre de processos en viu (supervisor -> worker -> nét)");
        Process p = startProcess(command("worker", "Arbre", "6", "0", "spawn"));
        FutureTask<List<LogLine>> out = drain(p.getInputStream(), "stdout");
        FutureTask<List<LogLine>> err = drain(p.getErrorStream(), "stderr");
        p.getOutputStream().close();

        long limit = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (p.descendants().findAny().isEmpty() && System.nanoTime() < limit) {
            Thread.sleep(50);
        }
        ProcessHandle me = ProcessHandle.current();
        System.out.println("  PID      PPID     Viu   Durada      Ordre");
        printTree(me, 0);
        ProcessHandle net = p.descendants().findFirst().orElse(null);
        Tree tree = new Tree(me.pid(), p.pid(), PPIDS.getOrDefault(p.pid(), -1L),
                net == null ? -1 : net.pid(),
                net == null ? -1 : net.parent().map(ProcessHandle::pid).orElse(-1L));

        terminate("ARBRE", p);
        collect(out);
        collect(err);
        ACTIVE.remove(p);
        return tree;
    }

    private static void printTree(ProcessHandle h, int nivell) {
        String cl = h.info().commandLine().orElse("?");
        int idx = cl.indexOf("Processos");
        String curta = idx >= 0 ? cl.substring(idx) : cl;
        long ppid = h.parent().map(ProcessHandle::pid).orElse(-1L);
        Duration d = h.info().startInstant().map(s -> Duration.between(s, Instant.now()))
                .orElse(Duration.ZERO);
        System.out.printf("  %-8d %-8d %-5s %-10s  %s%s%n", h.pid(), ppid, h.isAlive() ? "sí" : "no",
                d.toMillis() + " ms", "  ".repeat(nivell), curta);
        if (nivell == 0) {
            // Només ens interessen els processos creats per aquest supervisor.
            h.children().filter(c -> ACTIVE.stream().anyMatch(a -> a.pid() == c.pid()))
                    .forEach(c -> printTree(c, 1));
        } else {
            h.children().forEach(c -> printTree(c, nivell + 1));
        }
    }

    private static ProcessResult floodDemo() throws Exception {
        say("SUPERVISOR", "floodDemo: molta sortida sense bloquejar-se");
        ProcessResult r = supervise("WORKER-FLOOD", command("worker", "Flood", "1", "0", "flood"),
                null, Duration.ofSeconds(30));
        say("SUPERVISOR", "stdout=" + r.stdout().size() + " línies, stderr=" + r.stderr().size()
                + " línies, codi=" + r.exitCode() + ", durada=" + r.duration().toMillis() + " ms");
        return r;
    }

    private static ProcessResult cancelDemo() throws Exception {
        say("SUPERVISOR", "cancelDemo: cancel·lació a petició");
        Cancel cancel = new Cancel();
        Thread canceller = new Thread(() -> {
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                return;
            }
            cancel.request("petició externa");
        }, "canceller");
        canceller.start();
        ProcessResult r = supervise("WORKER-CANCEL",
                command("worker", "Cancel", "30", "0", "spawn"), null, Duration.ofSeconds(20), cancel);
        canceller.join();
        report("WORKER-CANCEL", r);
        say("SUPERVISOR", "causa del tancament: " + r.cause());
        return r;
    }

    private static List<String> command(String... roleAndArgs) {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        // UTF-8 explícit als fills: així els accents no depenen del locale del sistema.
        List<String> cmd = new ArrayList<>(List.of(java,
                "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8",
                "-cp", System.getProperty("java.class.path"), Processos.class.getName()));
        cmd.addAll(List.of(roleAndArgs));
        return cmd;
    }

    private static Process startProcess(List<String> cmd) throws IOException {
        synchronized (START_LOCK) {
            if (INTERROMPUT) {
                throw new IOException("supervisor interromput: no es creen processos nous");
            }
            Process p = new ProcessBuilder(cmd).start();
            PPIDS.put(p.pid(), p.toHandle().parent().map(ProcessHandle::pid).orElse(-1L));
            ACTIVE.add(p);
            return p;
        }
    }

    private static FutureTask<List<LogLine>> drain(InputStream in, String source) {
        FutureTask<List<LogLine>> task = new FutureTask<>(() -> {
            List<LogLine> lines = new ArrayList<>();
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    lines.add(new LogLine(Instant.now(), source, line));
                }
            }
            return lines;
        });
        Thread t = new Thread(task, "io-" + source);
        t.setDaemon(true);
        t.start();
        return task;
    }

    private static List<LogLine> collect(FutureTask<List<LogLine>> task) {
        try {
            return task.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return List.of();
        }
    }

    private static ProcessResult supervise(String label, List<String> cmd, String stdinData,
                                           Duration timeout) throws Exception {
        return supervise(label, cmd, stdinData, timeout, new Cancel());
    }

    private static ProcessResult supervise(String label, List<String> cmd, String stdinData,
                                           Duration timeout, Cancel cancel) throws Exception {
        Instant inici = Instant.now();
        Process p;
        try {
            p = startProcess(cmd);
        } catch (IOException e) {
            say("SUPERVISOR", label + ": no s'ha pogut iniciar (" + e.getMessage() + ")");
            return new ProcessResult(cmd, -1, ProcessHandle.current().pid(), -1, false,
                    "NO-INICIAT", Duration.between(inici, Instant.now()),
                    List.of(), List.of(new LogLine(Instant.now(), "stderr", e.getMessage())));
        }
        say("SUPERVISOR", label + " creat amb pid=" + p.pid() + " timeout=" + timeout.toSeconds() + "s");
        try {
            FutureTask<List<LogLine>> out = drain(p.getInputStream(), "stdout");
            FutureTask<List<LogLine>> err = drain(p.getErrorStream(), "stderr");

            try (BufferedWriter w = p.outputWriter(StandardCharsets.UTF_8)) {
                if (stdinData != null) {
                    w.write(stdinData);
                }
            } catch (IOException e) {
                say("SUPERVISOR", label + ": stdin tancat pel fill (" + e.getMessage() + ")");
            }

            long limit = System.nanoTime() + timeout.toNanos();
            String cause = "EXIT";
            while (!p.waitFor(100, TimeUnit.MILLISECONDS)) {
                if (cancel.cause != null) {
                    cause = cancel.cause;
                    break;
                }
                if (System.nanoTime() >= limit) {
                    cause = "TIMEOUT";
                    break;
                }
            }
            if (p.isAlive()) {
                say("SUPERVISOR", label + ": causa=" + cause + ", aturant el procés");
                terminate(label, p);
            } else {
                cause = INTERROMPUT ? "INTERRUPCIO" : "EXIT";
            }
            return buildResult(cmd, p, inici, cause, collect(out), collect(err));
        } finally {
            ACTIVE.remove(p);
        }
    }

    private static ProcessResult buildResult(List<String> cmd, Process p, Instant inici,
                                             String cause, List<LogLine> out,
                                             List<LogLine> err) throws InterruptedException {
        int codi = p.waitFor();
        ACTIVE.remove(p);
        return new ProcessResult(cmd, p.pid(), PPIDS.getOrDefault(p.pid(), -1L), codi,
                cause.equals("TIMEOUT"), cause, Duration.between(inici, Instant.now()), out, err);
    }

    private static void terminate(String label, Process p) throws InterruptedException {
        List<ProcessHandle> descendents = p.descendants().toList();
        DESCENDENTS_ATURATS = descendents.size();
        say("SUPERVISOR", label + ": SIGTERM a " + descendents.size()
                + " descendent(s) de pid=" + p.pid());
        descendents.forEach(ProcessHandle::destroy);
        for (ProcessHandle h : descendents) {
            try {
                h.onExit().get(2, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                say("SUPERVISOR", label + ": descendent pid=" + h.pid() + " no acaba, SIGKILL");
                h.destroyForcibly();
            }
        }
        // 2) Després el procés fill: ordenada (SIGTERM) i, si cal, forçada (SIGKILL).
        say("SUPERVISOR", label + ": SIGTERM a pid=" + p.pid());
        p.destroy();
        if (!p.waitFor(2, TimeUnit.SECONDS)) {
            say("SUPERVISOR", label + ": no ha acabat en 2 s, SIGKILL");
            p.destroyForcibly();
            p.waitFor();
        }
    }

    private static boolean isAlive(long pid) {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    private static OptionalLong netPid(ProcessResult r) {
        for (LogLine l : r.stdout()) {
            if (l.text().startsWith("NET pid=")) {
                return OptionalLong.of(Long.parseLong(l.text().substring("NET pid=".length()).trim()));
            }
        }
        return OptionalLong.empty();
    }

    private static synchronized void report(String label, ProcessResult r) {
        System.out.println("RESULTAT " + label + ":");
        System.out.println("  Ordre       : " + String.join(" ", r.command()));
        System.out.println("  Procés      : pid=" + r.pid() + " ppid=" + r.ppid());
        System.out.println("  Finalització: " + (r.timedOut() ? "TIMEOUT" : "codi " + r.exitCode())
                + "  causa=" + r.cause()
                + (!r.ok() ? "  <-- ERROR EXPLÍCIT" : ""));
        System.out.println("  Durada      : " + r.duration().toMillis() + " ms");
        String prefix = "  [" + label + " pid=" + r.pid() + "] ";
        int max = 8;
        for (int i = 0; i < Math.min(max, r.stdout().size()); i++) {
            System.out.println(prefix + r.stdout().get(i).format());
        }
        for (int i = 0; i < Math.min(max, r.stderr().size()); i++) {
            System.err.println(prefix + r.stderr().get(i).format());
        }
        System.out.println("  (stdout=" + r.stdout().size() + " línies, stderr="
                + r.stderr().size() + " línies)");
    }

    private static void printProcessTable(List<String> labels, List<ProcessResult> rs) {
        long me = ProcessHandle.current().pid();
        long mePpid = ProcessHandle.current().parent().map(ProcessHandle::pid).orElse(-1L);
        System.out.println("  Rol            PID      PPID     Estat       Codi");
        System.out.printf("  %-14s %-8d %-8d %-11s %s%n", "Supervisor", me, mePpid, "en marxa", "-");
        for (int i = 0; i < rs.size(); i++) {
            ProcessResult r = rs.get(i);
            System.out.printf("  %-14s %-8d %-8d %-11s %d%n", labels.get(i), r.pid(), r.ppid(),
                    r.cause().equals("EXIT") ? "acabat" : r.cause(), r.exitCode());
        }
    }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger n = new AtomicInteger(1);
        return r -> new Thread(r, prefix + "-" + n.getAndIncrement());
    }

    private static int passades;
    private static int fallades;

    private static void check(String nom, boolean condicio) {
        if (condicio) {
            passades++;
        } else {
            fallades++;
        }
        System.out.println("  [" + (condicio ? "OK " : "FALLA") + "] " + nom);
    }

    private static boolean runTests() throws Exception {
        say("SUPERVISOR", "BATERIA DE PROVES");

        ParallelOutcome par = parallelDemo();
        ProcessResult alpha = par.results().get(0);
        ProcessResult beta = par.results().get(1);
        check("èxit: Alpha acaba amb codi 0 i causa EXIT", alpha.ok());
        check("PPID del worker = PID del supervisor", alpha.ppid() == ProcessHandle.current().pid());
        check("codi 7: Beta retorna 7 i queda marcat com a error",
                beta.exitCode() == 7 && !beta.ok());
        check("els altres workers continuen: Gamma correcte", par.results().get(2).ok());
        check("límit: com a màxim 2 workers alhora (pic=" + par.peak() + ")", par.peak() == 2);
        check("stderr capturat amb origen", alpha.stderr().stream()
                .anyMatch(l -> l.source().equals("stderr") && l.text().contains("pas 1/3")));

        ProcessResult inexistent = supervise("INEXISTENT", List.of("/ruta/no/existeix"),
                null, Duration.ofSeconds(5));
        check("ordre inexistent: causa NO-INICIAT, sense excepció",
                inexistent.cause().equals("NO-INICIAT") && !inexistent.ok());

        ProcessResult eco = stdinDemo();
        check("stdin: l'echo retorna EBRE i PROCESSOS", eco.stdout().stream().map(LogLine::text).toList()
                .equals(List.of("EBRE", "PROCESSOS")));

        ProcessResult lent = timeoutDemo();
        check("timeout: causa TIMEOUT", lent.timedOut());
        check("timeout: en aturar-lo tenia 1 descendent viu (el nét existia de debò)",
                DESCENDENTS_ATURATS >= 1);
        check("timeout: el worker no queda viu", !isAlive(lent.pid()));
        check("timeout: el nét no queda viu", netPid(lent).isPresent()
                && !isAlive(netPid(lent).getAsLong()));

        ProcessResult[] pipe = pipeDemo();
        check("pipe: els dos codis són 0", pipe[0].exitCode() == 0 && pipe[1].exitCode() == 0);
        check("pipe: COUNT=5 CHARS=18", pipe[1].stdout().stream()
                .anyMatch(l -> l.text().equals("COUNT=5 CHARS=18")));

        ProcessResult canc = cancelDemo();
        check("cancel·lació: causa CANCEL", canc.cause().startsWith("CANCEL"));
        check("cancel·lació: en aturar-lo tenia 1 descendent viu", DESCENDENTS_ATURATS >= 1);
        check("cancel·lació: ni worker ni nét queden vius", !isAlive(canc.pid())
                && netPid(canc).isPresent() && !isAlive(netPid(canc).getAsLong()));

        Tree arbre = arbreDemo();
        check("arbre: el worker és fill del supervisor (PPID observat)",
                arbre.workerParent() == arbre.supervisor());
        check("arbre: el nét és fill del worker i no del supervisor",
                arbre.net() > 0 && arbre.netParent() == arbre.worker());

        ProcessResult flood = floodDemo();
        check("bloqueig evitat: 50.000 línies per flux sense penjar-se",
                flood.stdout().size() >= 50_000 && flood.stderr().size() >= 50_000 && flood.ok());

        check("cap procés actiu al final", ACTIVE.isEmpty());
        say("SUPERVISOR", "PROVES: " + passades + " correctes, " + fallades + " fallades");
        return fallades == 0;
    }

    private static void runWorker(String[] args) throws Exception {
        String nom = args.length > 1 ? args[1] : "X";
        int segons = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        int codi = args.length > 3 ? Integer.parseInt(args[3]) : 0;
        String mode = args.length > 4 ? args[4] : "normal";
        String rol = "WORKER-" + nom.toUpperCase(Locale.ROOT);

        if (mode.equals("spawn")) {
            Process net = new ProcessBuilder(command("worker", "Net", "300", "0", "normal"))
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD).start();
            System.out.println("NET pid=" + net.pid());
            System.out.flush();
        }
        if (mode.equals("flood")) {
            for (int i = 0; i < 50_000; i++) {
                System.out.println("línia stdout " + i);
                System.err.println("línia stderr " + i);
            }
        }
        for (int pas = 1; pas <= 3; pas++) {
            System.err.println(diag(rol, "pas " + pas + "/3"));
            Thread.sleep(segons * 1000L / 3);
        }
        System.out.println("RESULTAT " + nom + " acabat amb codi " + codi);
        System.exit(codi);
    }

    private static void runProducer() throws Exception {
        System.err.println(diag("PRODUCER", "inici"));
        for (String nom : List.of("Anna", "Pau", "Marc", "Joan", "Eva")) {
            System.out.println(nom);
            System.out.flush();
            Thread.sleep(200);
        }
        System.err.println(diag("PRODUCER", "fi, 5 noms enviats"));
    }

    private static void runConsumer() throws IOException {
        System.err.println(diag("CONSUMER", "inici"));
        int linies = 0;
        int caracters = 0;
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                linies++;
                caracters += line.length();
            }
        }
        System.out.println("COUNT=" + linies + " CHARS=" + caracters);
        System.err.println(diag("CONSUMER", "EOF rebut, fi"));
    }

    private static void runEcho() throws IOException {
        System.err.println(diag("ECHO", "inici"));
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null && !line.equals("FI")) {
                System.out.println(line.toUpperCase(Locale.ROOT));
            }
        }
    }
}
