**Problema:**

O output final do workflow de agentes em LangGraph, `20250825_104802_vr_final_report.xlsx`, está incorreto e não adere às especificações do projeto. As inconsistências identificadas são:

1.  **Ausência da Matrícula do Colaborador:** A identificação única de cada colaborador não está presente no relatório final, impossibilitando a correta associação dos dados.
2.  **Inacurácia dos Dados:** O arquivo de saída apresenta valores duplicados e ilógicos entre as colunas, indicando falhas no cálculo e na lógica de processamento dos agentes.
3.  **Não conformidade com o `descricao.md`:** O resultado gerado diverge do formato e da estrutura de entrega final detalhados no arquivo de descrição do projeto.

**Tarefa:**

1.  **Estudo e Análise:** Estude bastante e profundamente os arquivos de projeto com as seguintes prioridades:
    * **`descricao.md`:** Para compreender os objetivos, diretrizes e o formato mandatório da entrega final e esperada do output.
    * **`README.md`:** Para verificar a lógica e o fluxo de agentes existentes, a fim de diagnosticar o erro atual.
    * **`@estrutura_inputs.md`:** Para entender a estrutura dos 11 arquivos de dados de entrada e guiar a nova lógica de processamento.
2.  **Desenvolvimento da Lógica:** Com base na análise, desenvolva uma nova lógica de programação precisa para os agentes em LangGraph. Esta lógica deve ser capaz de:
    * Identificar unicamente cada colaborador através de sua **matrícula**.
    * Calcular, para **cada colaborador individualmente**, o valor exato do Vale Refeição (VR) a ser concedido.
    * Determinar com acuracidade a parcela do valor do VR a ser custeada pela empresa e a parcela a ser descontada do colaborador.
    Lembre-se que o resultado desta lógica deve gerar a resposta final no formato especificado na seção **"Entrega final"** do arquivo `descricao.md`.

3.  **Implementação e Validação:** Implemente a nova lógica no sistema multiagentes, garantindo que o output final (`vr_final_report.xlsx`) seja gerado estritamente conforme o formato especificado no `descricao.md`, sem valores repetidos e com os dados corretos e validados para cada matrícula.