import numpy as np


def cosine_similarity(embedding1, embedding2):
    """
    Calcula la similitud del coseno entre dos embeddings.

    :param embedding1: Primer embedding (vector).
    :param embedding2: Segundo embedding (vector).
    :return: Similitud del coseno entre los dos embeddings.
    """
    dot_product = np.dot(embedding1, embedding2)
    norm_embedding1 = np.linalg.norm(embedding1)
    norm_embedding2 = np.linalg.norm(embedding2)

    if norm_embedding1 == 0 or norm_embedding2 == 0:
        return 0.0  # Evitar división por cero

    return dot_product / (norm_embedding1 * norm_embedding2)


from openai import OpenAI

client = OpenAI(base_url="http://host.docker.internal:1234/v1", api_key="lm-studio")


def get_embedding(text, model="nomic-ai/nomic-embed-text-v1.5-GGUF"):
    text = text.replace("\n", " ")
    response=client.embeddings.create(input=[text], model=model)
    print(response.usage.total_tokens)
    
    return response.data[0].embedding


text = """
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

# Ejemplo de uso
embedding_string = np.array(get_embedding(text))  # Embedding del primer string
embedding_query = np.array(
    get_embedding(
        "{Es de tipo .txt}Tecnicas de fine tuning de modelos para chatbots y como se pueden construir las querys"
    )
)  # Embedding del segundo string

similitud = cosine_similarity(embedding_string, embedding_query)
print(f"La similitud del coseno entre los embeddings es: {similitud}")
