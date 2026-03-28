import streamlit as st

st.set_page_config(page_title="PUCRS • PNADC IBGE Dashboard", page_icon="📊", layout="wide")

st.markdown("""
<style>
.stApp {
    background: radial-gradient(circle at top right, rgba(66,215,255,.10), transparent 20%), linear-gradient(180deg, #06111f 0%, #091a31 50%, #071427 100%);
    color: white;
}
.block-container {padding-top: 1.3rem; padding-bottom: 1rem; max-width: 1400px;}
.card {
    background: linear-gradient(180deg, rgba(16,41,73,.85), rgba(10,26,48,.90));
    border: 1px solid rgba(110,176,255,.18);
    border-radius: 18px;
    padding: 1.05rem 1.2rem;
    box-shadow: 0 18px 38px rgba(0,0,0,.28);
}
.small {color:#b8c7d9; font-size: .95rem;}
.metric {
    background: rgba(255,255,255,.03);
    border: 1px solid rgba(110,176,255,.12);
    border-radius: 14px;
    padding: .9rem 1rem;
}
</style>
""", unsafe_allow_html=True)

st.image("dashboard_banner_pucrs_jupyter.png", use_container_width=True)
st.markdown("""
<div class="card">
  <h1 style="margin:0; color:white;">PUCRS • Análise de Dados PNADC IBGE</h1>
  <p class="small">Versão interativa super plus para navegação executiva do notebook. Esta entrega preserva o notebook original e adiciona uma camada de apresentação para exploração.</p>
</div>
""", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
items = [("Hipóteses","H1–H7"),("Tema","BI Dark"),("Fonte","GitHub"),("Entrega","Notebook + Banner")]
for col, item in zip((c1,c2,c3,c4), items):
    title, value = item
    with col:
        st.markdown(f"<div class='metric'><div class='small'>{title}</div><div style='font-size:1.3rem;font-weight:800'>{value}</div></div>", unsafe_allow_html=True)

tabs = st.tabs(["Visão geral", "Mapa do notebook", "Como usar"])
with tabs[0]:
    st.markdown("""
    <div class="card">
      <h3 style="margin-top:0;">Painel executivo</h3>
      <p class="small">A versão pronta principal está no arquivo <code>notebook_pucrs_dashboard_superplus.ipynb</code>. Ela já vem com banner, tema visual BI, cards de seção, navegação rápida e botão para ocultar/exibir códigos.</p>
      <ul>
        <li>Sem alteração da lógica analítica</li>
        <li>Sem alteração das descrições originais</li>
        <li>Com reforço visual para apresentação acadêmica</li>
      </ul>
    </div>
    """, unsafe_allow_html=True)

with tabs[1]:
    st.markdown("""
    <div class="card">
      <h3 style="margin-top:0;">Seções</h3>
      <ol>
        <li>H1 — Renda por escolaridade</li>
        <li>H2 — Retorno da escolaridade</li>
        <li>H3 — Estrutura ocupacional</li>
        <li>H4 — Formalização</li>
        <li>H5 — Estrutura ocupacional + renda</li>
        <li>H6 — Jovens no mercado de trabalho</li>
        <li>H7 — Cargos de liderança</li>
        <li>Leitura acadêmica dos resultados</li>
      </ol>
    </div>
    """, unsafe_allow_html=True)

with tabs[2]:
    st.markdown("""
    <div class="card">
      <h3 style="margin-top:0;">Execução</h3>
      <p class="small">Abra o notebook pronto no Jupyter e execute normalmente. O dashboard visual aparece já no topo. Para a camada Streamlit, rode:</p>
      <pre>streamlit run dashboard_superplus_app.py</pre>
    </div>
    """, unsafe_allow_html=True)
