# Snippets Seguros (Public Showcase)

Esta pasta organiza recortes de codigo seguros para publicacao publica.

Objetivo:
- mostrar padroes de engenharia;
- evitar vazamento de segredos e logica proprietaria;
- facilitar leitura por recrutadores, clientes e equipe tecnica.

## Estrutura

- `01-defensive-ai/`: guardrails de seguranca, validacao de argumentos, anti prompt injection.
- `02-regression-tests/`: scripts de regressao e cenarios de validacao.
- `03-checkout-flow/`: fluxos de checkout sem friccao e tratamento de contexto.
- `_templates/`: modelos para novos snippets.

## Regras de publicacao

1. Remover tokens, URLs privadas, IDs de cliente e qualquer dado sensivel.
2. Trocar nomes reais por placeholders (`TENANT_ID_EXAMPLE`, `INSTANCE_TOKEN_EXAMPLE`).
3. Manter snippets autocontidos e com dependencia minima.
4. Incluir contexto de entrada/saida no README local do snippet.
5. Nao publicar regras de negocio proprietarias completas.

## Header recomendado para cada snippet

Use o template em `_templates/snippet_header.md`.
