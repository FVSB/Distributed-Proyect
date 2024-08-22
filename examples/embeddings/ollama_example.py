import ollama
from ollama import Options
import numpy as np

# Configura la conexión al servidor local de Ollama
client = ollama.Client(host="http://host.docker.internal:11434")

# Lista de documentos
prompt="""
{Es de tipo .txt}
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
user questio
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given


{Es de tipo .txt}
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
user questio
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
Fine-Tuning Techniques: Fine-tuning involves extending the capabilities of language models by directly modifying their weights
and/or architecture. This can be done efficiently without requiring
the training of models from scratch, enabling the rapid development
of more advanced language models or specializing smaller models in
domains where even larger models don’t work as well.
Part 3: Applications
The final and largest part of the book is dedicated to building applications
that leverage large language models. We will create a dozen or so applications, ranging from simple chat-based tools to more complex systems,
demonstrating the potential of these models in real-world scenarios. Some
of the applications we will build include:
1. Chatbots: Typical chatbots that can engage in meaningful conversations with users, providing information, and answering questions
in traditional scenarios such as customer service.
2. Text Analysys Tools: Tools that can summarize long texts, extract insights, and answer specific questions while providing references.
3. Coding Helpers: Coding assistance tools, that can generate, modify, explain, and debug code, as well as translating code between
different programming languages.
4. Data Analysis Tools: Tools that can analyze structured data, such
as tables, and provide accuracte analytics, including predictions and
visualizations.
5. Writing Assistants: Tools that can enhance your writing providing ideas, outlines, and full drafts, as well as editing and criticizing
existing text.
6. Research Assistants: Tools that can search the web for some
specific information and build semi-structured reports on a given
"""

# Genera embeddings para cada documento
a=client.embeddings(model='llama3' ,prompt=prompt,keep_alive=-1)
print(a)
print(type(a))
b=a['embedding']
print(len(b))
embeddings = b

# Convierte los embeddings a un array de NumPy
embeddings_array = np.array(embeddings)


