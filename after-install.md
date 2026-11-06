# Telegram Guest Mode installed

1. Confirm `hermes --version` reports `v0.23.0` or newer (the plugin requires the Telegram environment bridge fields in the native platform registry).
2. The gateway must be restarted to load the installed plugin. Restart it yourself; the installer agent must not restart it on your behalf.
3. Verify normal connectivity with `hermes gateway status`.
4. In [@BotFather](https://t.me/BotFather), open your bot's **Bot Settings → Guest Mode** and enable it.
5. Mention the bot in a Telegram conversation where it is not a member.

If Telegram does not reconnect, restore the bundled adapter:

```bash
hermes plugins disable telegram-guest
```

Then restart the gateway yourself and confirm its status before removing the plugin.
