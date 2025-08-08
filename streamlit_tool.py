class StreamlitOutputRedirector:
        def __init__(self, placeholder):
            self.placeholder = placeholder
            self.buffer = []

        def write(self, message):
            self.buffer.append(message)
            # You can choose how to display the output, e.g., st.text, st.markdown, etc.
            # Here, we join the buffer and update the placeholder.
            self.placeholder.text("".join(self.buffer))

        def flush(self):
            # This method is required for file-like objects but can be empty for this purpose.
            pass