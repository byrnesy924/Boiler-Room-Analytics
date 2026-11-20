# Outline
This is an initial outline document that. It covers the basic design of the front end

# Goals
The front end needs to be:
- Sleek, modern and minimalist in aesthetic
- It needs to render a complex graph
    - My finger in the wind estimate is to contain around 200k edges and around 20-20k nodes at maximum
    - It can do this by rendering parts at a time (i.e., major nodes and then only loading smaller nodes when zoomed in)
- It will have an LLM chat feature that uses a GraphRAG architecture
    - The ability to traverse the graph needs to be relatively fast, but this may be a backend feature only
- Traversing the graph eloquently from the front end is a bonus

# Basic tech stack
- Vite for front end build w/ Tailwind CSS
- Graphology + Sigma.js for rending graph
  - Alternative to use cosmos.gl if the graph grows in size