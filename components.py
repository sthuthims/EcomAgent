def render_chat_message(message, is_user=False):
    """Render a chat message with the appropriate styling"""
    style_class = "user-message" if is_user else "assistant-message"
    icon = "👤" if is_user else "🤖"
    
    return f"""
        <div class="chat-message {style_class}">
            <div style="display: flex; align-items: start; gap: 1rem;">
                <div style="font-size: 1.5rem;">{icon}</div>
                <div>{message}</div>
            </div>
        </div>
    """