import asyncio
import pandas as pd
import random
import re
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

def limpiar_texto(texto):
    if not texto: return "-"
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()

def extraer_valor(titulo, clase_padre, clase_valor, split_razon_social=False):
    contenedor_padre = titulo.find_parent("div", class_=clase_padre)
    if contenedor_padre:
        valor = contenedor_padre.find_next_sibling("div", class_=clase_valor)
        if valor:
            texto = limpiar_texto(valor.get_text())
            if split_razon_social:
                return limpiar_texto(texto.split("-", 1)[-1])
            return texto
    return None

async def consultar():
    try:
        df_ruc = pd.read_excel("ruc_list.xlsx", sheet_name="RUCs")
        ruc_list = df_ruc["RUC"].astype(str).str.strip().tolist()
    except Exception as e:
        print(f"❌ Error al leer Excel: {e}")
        return

    resultados = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        for index, RUC in enumerate(ruc_list):
            if not RUC or RUC == "nan": continue

            print(f"[{index+1}/{len(ruc_list)}] Consultando RUC: {RUC}")

            try:
                await page.goto("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp")
                await page.wait_for_selector("#txtRuc")
                await page.fill("#txtRuc", RUC)
                await page.click("#btnAceptar")

                try:
                    await page.wait_for_selector(".list-group", timeout=8000)
                except:
                    print(f"⚠️ RUC {RUC} no cargó.")
                    resultados.append({"RUC": RUC, "Razon Social": "NO ENCONTRADO"})
                    continue

                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                datos = {
                    "RUC": RUC,
                    "Razon Social": "-",
                    "Fecha Inicio": "-",
                    "Estado": "-",
                    "Condicion": "-",
                    "Domicilio Fiscal": "-",
                    "Comercio Exterior": "-",
                    "Actividades Economicas": "-",
                    "Sistemas de Emision": "-",
                    "Padrones": "-"
                }

                titulos = soup.find_all("h4", class_="list-group-item-heading")

                for t in titulos:
                    txt_titulo = t.get_text(strip=True)

                    # 🔹 Fecha Inicio
                    if "Fecha de Inicio de Actividades:" in txt_titulo:
                        val = extraer_valor(t, "col-sm-3", "col-sm-3")
                        if val: datos["Fecha Inicio"] = val

                    # 🔹 Comercio Exterior
                    elif "Actividad Comercio Exterior:" in txt_titulo:
                        val = extraer_valor(t, "col-sm-3", "col-sm-3")
                        if val: datos["Comercio Exterior"] = val

                    # 🔹 Razón Social
                    elif "Número de RUC:" in txt_titulo:
                        val = extraer_valor(t, "col-sm-5", "col-sm-7", split_razon_social=True)
                        if val: datos["Razon Social"] = val

                    # 🔹 Estado
                    elif "Estado del Contribuyente:" in txt_titulo:
                        val = extraer_valor(t, "col-sm-5", "col-sm-7")
                        if val: datos["Estado"] = val

                    # 🔹 Condición
                    elif "Condición del Contribuyente:" in txt_titulo:
                        val = extraer_valor(t, "col-sm-5", "col-sm-7")
                        if val: datos["Condicion"] = val

                    # 🔹 Domicilio
                    elif "Domicilio Fiscal:" in txt_titulo:
                        val = extraer_valor(t, "col-sm-5", "col-sm-7")
                        if val: datos["Domicilio Fiscal"] = val

                    # 🔹 Tablas
                    elif "Actividad(es) Económica(s):" in txt_titulo:
                        bloque_item = t.find_parent("div", class_="list-group-item")
                        tabla = bloque_item.find("table", class_="tblResultado")
                        if tabla:
                            acts = [limpiar_texto(td.get_text()) for td in tabla.find_all("td")]
                            datos["Actividades Economicas"] = " | ".join(acts)

                    elif "Sistema de Emisión Electrónica:" in txt_titulo:
                        bloque_item = t.find_parent("div", class_="list-group-item")
                        tabla = bloque_item.find("table", class_="tblResultado")
                        if tabla:
                            sists = [limpiar_texto(td.get_text()) for td in tabla.find_all("td")]
                            datos["Sistemas de Emision"] = " | ".join(sists)

                resultados.append(datos)
                await asyncio.sleep(random.uniform(2, 3))

            except Exception as e:
                print(f"❌ Error en {RUC}: {e}")

        await browser.close()

    df_final = pd.DataFrame(resultados)
    df_final.to_excel("Resultado_SUNAT_Final.xlsx", index=False)
    print("\n🚀 ¡Listo! Revisa 'Resultado_SUNAT_Final.xlsx'")

if __name__ == "__main__":
    asyncio.run(consultar())