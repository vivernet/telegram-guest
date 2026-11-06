# Agent installation guide

This repository is a standalone Hermes Agent plugin. When a user explicitly asks you to install it:

1. Inspect `README.md`, `plugin.yaml`, and `adapter.py` before making changes.
2. Resolve the active Hermes profile and inspect whether `telegram-guest` is already installed.
3. Never overwrite an existing plugin blindly. If it exists, inspect its Git origin and working tree before deciding whether to update or stop.
4. Confirm Hermes is v0.23.0 or newer; `telegram-guest` requires the native Telegram env bridge fields.
5. Install only from `vivernet/telegram-guest`. For a managed profile, use the full SHA in its manifest; otherwise resolve and verify a full SHA from the fork before installing with `hermes plugins install vivernet/telegram-guest --ref <full-SHA> --enable`.
6. Tell the user to restart the gateway themselves, then verify `hermes gateway status` and normal Telegram connectivity. Never restart/stop the user's gateway on their behalf. Guest Mode still has to be enabled separately in BotFather.
7. If startup or Telegram connectivity fails, run `hermes plugins disable telegram-guest` only when requested, then tell the user to restart the gateway to restore the bundled adapter.

Do not edit the Hermes core checkout to install this plugin. Do not request or expose the Telegram bot token.
