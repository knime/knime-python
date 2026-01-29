Zu dem `Execute failed: Cannot read DataCell with empty type information` Error.

Das passiert genau ab pandas 2.1.0 (wie das Problem mit den leeren Tables auch).

In der SDK ist der Error: `Cannot read the array length because "bytes" is null`. Warum ein andererer Error? In production ist `arrow.enable_null_check_for_get=false` und wir lesen einfach aus dem byte array und bekommen ein leeres byte array obwohl das validity bit auf "false" steht.

Das ist das Struct Dict Encoding Array sieht so aus:
- indices: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ... 0, 0, 0, 0, 0, 0, 0, 0, 0, null]
- data: [null, null, null, null, null, null, null, null, null, null, ... null, null, null, null, null, null, null, null, null, null]]
- NOTE: Das data Array ist komplett null, aber sollte einen wert auf index 0 haben, weil darauf alle anderen indices verweisen.

Carsten hat schon angebracht, dass es irgendwie mit dem batching zu tun hat.

Ich hab mal die batch boundaries ausgegeben:
- Mit pandas 2.0.3: `256,512,768,998`
- Mit pandas 2.1.0: `590,998`

Ich weiß noch nicht genau was ich damit anfangen soll aber meine aktuelle Theorie ist, dass neu irgendwo ein re-batching stattfindet, dass unsere Struct-Dict-Encoded Arrays auseinander schneidet ohne, dass die indices angepasst werden.
Mir ist aber nicht klar, warum dann in den array von oben (der 2te batch mit einer Länge von 408) alle data values null sind anstatt non-null an den alten batch Grenzen.
