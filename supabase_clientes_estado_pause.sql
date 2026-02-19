-- Adiciona suporte para pausa de IA por conversa no painel admin
ALTER TABLE IF EXISTS public.clientes_estado
  ADD COLUMN IF NOT EXISTS ai_paused_until timestamptz;

CREATE INDEX IF NOT EXISTS idx_clientes_estado_ai_paused_until
  ON public.clientes_estado(ai_paused_until);
