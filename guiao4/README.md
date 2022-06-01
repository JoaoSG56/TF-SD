# Tolerância a Faltas - Lab 4

## Garantir que não existe re-execução de transações e permitir execução não deterministica
Para garantir que não existe re-execução de transações e permitir execução não deterministica iremos utilizar replicação passiva. Deste modo, a maquina que receber o pedido de transação irá calcular o resultado e irá envia-lo a todas as réplicas, de modo a estas não re-executarem e a poderem ser executadas operações não-deterministicas (sendo a operação efetuada uma vez, o resultado será o mesmo em todas as bases de dados). 

## Garantir a total order
Para garintir a total order utilizamos o serviço "lin-tso" do maelstrom
Para isso, definimos em cada servidor um timestamp global (current) em que uma mensagem só é executada quando o seu ts é igual ao current. Se o ts da mensagem for maior que o current, esta é guardada num dicionário (messages_out_of_order) para ser processada na sua vez
Após processar uma mensagem o current é incrementado e verificamos se a mensagem com ts correspondente a esse current incrementado está no dicionário. Se estiver, processamos essa mensagem e assim recursivamente até a mensagem seguinte não estar no dicionario

## Anexar ts a mensagem txn
Quando uma mensagem "txn" chega ao servidor, metemos essa mensagem numa queue (messages_no_ts) e enviamos um pedido "ts"
Ao receber o "ts_ok" removemos a mensagem na queue e anexamos o timestamp recebido a mensagem, antes de procedermos para a transition

## Processar a mensagem txn
A mensagem é processada pela função "process_transition".
Primeiramente, é verificado se o ts é igual ao current. Caso não seja, é adicionado ao dicionario como foi descrito em cima. Caso seja, é verificado que o tipo da mensagem é "txn" e avançamos para a transição.
Começamos por adquirir os locks da base de dados com a função begin e fazemos o execute da query.
Verificamos se o resultado (res) é valido ou não. Se for válido, damos commit da operação na base de dados e enviamos uma mensagem "txn_replication" a todas as replicas incluindo a propria maquina, com o resultado do execute da query.
Se não for válido, apenas enviamos uma mensagem "txn_aborted" a todas as replicas, incluindo a propria maquina.
Por ultimo, libertamos os locks da base de dados e fazemos o processo de aumentar o current e verificar se existe a proxima mensagem no dicionario, como foi descrito em cima.

## Processar a mensagem txn_replication
Primeiramente verificamos se a mensagem é do próprio servidor. Se sim, este enviará uma mensagem "txn_ok" ao cliente com o resultado.
Caso contrário, a mensagem também é processada pela função "process_transition", logo o processo inicial de verficar se o ts e o current são iguais e a parte final de verificar se a proxima mensagem esta no dicionario são iguais ao processo anterior.
Quanto a processar a transição, depois de verificar que o tipo da mensagem é "txn_replication", começamos por adquirir os locks da base de dados. Após adquirir os locks damos commit do resultado da operação que se encontra no corpo da mensagem e libertamos os locks.

## Processar a mensagem txn_aborted
Primeiramente verificamos se a mensagem é do próprio servidor. Se sim, este enviará uma mensagem "error" com codigo 14 ao cliente a informar que a transação foi abortada.
Caso contrário, a mensagem também é processada pela função "process_transition", logo o processo inicial e final da função são iguais ao processo anterior.
Para este tipo de mensagem apenas se incrementa o current, de modo a não parar a execução do programa e verifica-se se a proxima mensagem está no dicionário.