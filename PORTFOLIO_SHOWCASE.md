# PORTFOLIO SHOWCASE GUIDE

## Objetivo

Este guia organiza o repositorio para avaliacao tecnica em entrevistas, com foco em:
- arquitetura orientada a operacao real;
- padroes de seguranca para agentes de IA;
- engenharia de confiabilidade (regressao e validacao);
- fluxo de checkout robusto e auditavel.

## O que mostrar primeiro

1. `README.md` para visao de produto e arquitetura.
2. `snippets/01-defensive-ai/v2_guard_tool_args_showcase.py` para guardrails.
3. `snippets/02-regression-tests/agent_v2_regression_showcase.py` para estrategia de testes.
4. `snippets/03-checkout-flow/checkout_transition_showcase.py` para estado de pedido.

## Narrativa recomendada em entrevista

1. Problema: atendimento em pico gera erro, fila e abandono.
2. Solucao: IA conversacional com backend deterministico e estado por sessao.
3. Confiabilidade: fallbacks, parser defensivo, regressao orientada a casos reais.
4. Seguranca: bloqueio de parametros invalidos e contratos estritos por tool.
5. Resultado: operacao escalavel sem depender de atendimento humano 1:1.

## Limite de exposicao 
 
Este repositório possui partes operacionais internas. Para portfolio publico, priorize a pasta `snippets/` e a documentacao.

## Checklist rapido de publicacao

- [ ] Revisar se nao ha chaves e segredos em arquivos versionados.
- [ ] Confirmar que logs e dumps sensiveis nao estao sendo publicados.
- [ ] Publicar com foco em snippets, arquitetura e resultados tecnicos.
- [ ] Manter o disclaimer de showcase no README.
