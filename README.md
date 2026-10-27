# Pràctica 01 · Supervisor de processos i comunicació IPC

MP 0490 · Programació de serveis i processos · AEA1 · RA1

## Autoria

- Cinta Castelló Escurriola
- **Ús d'IA i recursos utilitzats:** s'ha usat Claude (Anthropic) per revisar el codi, detectar errors i fer recerca ràpida d'Internet. A més, s'han utilitzat els recursos proporcionats pel professorat i foros d'internet.

## Com funciona

Un únic programa Java (`src/Processos.java`) que fa de supervisor i de fills: el supervisor es relança a si mateix amb un rol diferent (`worker`, `producer`, `consumer`, `echo`) i cada fill és un procés real amb PID propi.

- Crea els processos amb `ProcessBuilder` i una llista d'arguments.
- Llegeix `stdout` i `stderr` alhora, amb marca temporal i origen, i envia dades a `stdin` tancant-lo després.
- Executa 3 workers amb un màxim de 2 alhora.
- Aplica timeout i cancel·lació amb finalització ordenada i, si cal, forçada, també dels descendents. Un hook de tancament cancel·la els fills si el supervisor s'interromp.
- Implementa la pipe producer --> supervisor --> consumer.
- Retorna cada execució en un resultat immutable (ordre, PID, PPID, codi, durada, sortides i timeout).

## Requisits

- JDK 21 (java -version)
- Linux amb bash


## Arrencada

bash
chmod +x executa.sh     # només la primera vegada
./executa.sh            # demostració completa
./executa.sh pipe       # un escenari concret


Escenaris: demo (per defecte), parallel, stdin, timeout, pipe, flood, cancel, arbre, tests.

Si es prefereix a mà:

bash
mkdir -p out && javac -d out src/Processos.java
java -Descenari=parallel -cp out Processos

## Proves

bash
./executa.sh tests


Executa 21 comprovacions automàtiques (èxit, codi 7, límit de 2 concurrents, ordre inexistent, stderr, stdin, timeout, pipe, cancel·lació, arbre de processos). Acaba amb codi 0 si totes passen i amb 1 si en falla alguna.

## Documentació extesa

La resta de documentació més detallada es pot trobar al document pdf enviat al professor. Hi son les decisions, incidències, activitats d'anàlisi.
