from pathlib import Path
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup
import re
import json
import time
from core.base_spider import BaseSpider, MixTab, ScrapeResult
from utils.drission_scraper.drission_scraper import DrissionScraperSession

import requests

class UniversityOfOtagoPgSpider(BaseSpider):
    school_name = 'University_of_Otago'
    major_level = 'pg'

    def __init__(self):
        super().__init__(self.school_name, self.major_level, max_workers=10)
        # 用于存储爬取过程中的专业数据
        self.crawled_majors = []

    def initialize(self):

        print("====== 初始化开始 ======")

        # ------- 统一默认值，防止异常中断 -------
   
        self.application_start_date = ""
        self.application_deadline = ""
        self.start_date = ""
        self.IELTS = ""
        self.TOEFL = ""
        self.PTE = ""

        browser = self._get_browser()



        # =====================================================================
        # 2）申请日期
        # =====================================================================
        print(" 获取申请日期...")

        try:
            tab = browser.new_tab()
            tab.get("https://www.otago.ac.nz/international/future-students/prepare-for-otago/key-dates-for-new-international-students", timeout=40)
            tab.wait.doc_loaded()

            # 开始时间
            try:
                self.application_start_date = tab.ele('css:#table62309r1c1').text
            except:
                self.application_start_date = ""

            # 截止日期
            try:
                d1 = tab.ele('css:#table62309r6c1').text
                d2 = tab.ele('css:#table29326r3c1').text
                self.application_deadline = f"{d1}  {d2}"
            except:
                self.application_deadline = ""

            # 开学日期
            try:
                texts = [e.text for e in tab.eles('x://p[contains(.,"semester")]')]
                s1, s2 = "", ""
                for t in texts:
                    if "semester 1" in t.lower():
                        s1 = t
                    if "semester 2" in t.lower():
                        s2 = t
                self.start_date = f"{s1} {s2}".strip()
            except:
                self.start_date = ""

            tab.close()
            print(" 申请日期完成")

        except Exception as e:
            print(" 申请日期失败:", e)
            self.application_start_date = ""
            self.application_deadline = ""
            self.start_date = ""
            try:
                tab.close()
            except:
                pass

        # =====================================================================
        # 3）语言要求
        # =====================================================================
        print(" 获取语言要求...")

        try:
            tab = browser.new_tab()
            tab.get("https://www.otago.ac.nz/future-students/entry-requirements/language-requirements", timeout=40)
            tab.wait.doc_loaded()

            # IELTS
            try:
                self.IELTS = tab.ele('x://td[contains(.,"IELTS")]/following-sibling::td[2]').text
            except:
                self.IELTS = ""

            # TOEFL
            try:
                self.TOEFL = tab.ele('x://td[contains(.,"TOEFL")]/following-sibling::td[2]').text
            except:
                self.TOEFL = ""

            # PTE
            try:
                self.PTE = tab.ele('x://td[contains(.,"PTE")]/following-sibling::td[2]').text
            except:
                self.PTE = ""

            tab.close()
            print("语言要求完成")

        except Exception as e:
            print("语言要求失败:", e)
            self.IELTS = ""
            self.TOEFL = ""
            self.PTE = ""
            try:
                tab.close()
            except:
                pass

        print("====== 初始化完成 ======")
        print("IELTS:", self.IELTS)
        print("TOEFL:", self.TOEFL)
        print("PTE:", self.PTE)
        print("Start date:", self.start_date)



    # 1）获取专业列表（从 sitemap）
    def get_list_urls(self, tab: MixTab) -> Optional[List[Dict[str, Any]]]:
        sitemap_path = Path(__file__).parent / 'sitemap_pg.json'
        
        
        sitemap = self.get_json_data(sitemap_path)

        scraper = DrissionScraperSession()
        major_list = scraper.run(sitemap, False)

        return major_list

    # ==============================
    # 2）解析详情页
    # ==============================
    def scrape_detail_page(self, page: MixTab, major_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        err_list = []
        major_url = major_info.get("major_url-href", "")

        if not major_url:
            return ScrapeResult(
                data={},
                err_info={"url": "", "err_list": ["major_url-href为空"]}
            )

        try:
            page.get(major_url)
            # 等待文档加载完成
            page.wait.doc_loaded()
            # 增加固定等待时间，让页面完全加载
            page.wait(4)  # 等待4秒
        except Exception as e:
            return ScrapeResult(
                data={"major_url": major_url},
                err_info={"url": major_url, "err_list": [f"页面加载失败: {e}"]}
            )
        # ========== 专业名称 major_name ==========
        try:
            h1_text = page.ele("x://h1[@class='page-banner__title']", timeout=10).text.strip()
            h3_text = ""
            try:
                h3_element = page.ele("x://h3[@data-role='banner-major-title']", timeout=2)
                if h3_element:
                    h3_text = h3_element.text.strip()
            except:
                pass

            if h3_text:
                major_name = f"{h1_text} {h3_text}".strip()
            else:
                major_name = h1_text


            major_name_lower = major_name.lower()
            if any(t in major_name_lower for t in ['doctor of philosophy', 'phd', 'bachelor']):
                print(f"直接跳过专业: {major_name}")
                return None
           

        except Exception as e:
            err_list.append(f"专业名称获取失败——{e}")
            major_name = ""



        
        # 初始化变量
        academic_requirements = ""
        entry_require_general_desc = ""
        entry_require = ""
        course_struct_desc = ""
        degree = ""
        faculty_name = ""
        overview = ""
        study_mode = ""
        apply_url = ""
        expected_duration = ""
        fees = ""
        language_require = ""
        
        # ---------- 学术要求 ----------
        try:
            academic_ele = page.ele('x://h3[contains(text(), "Admission to the Programme")]/following-sibling::ol[1]', timeout=5)
            if academic_ele:
                academic_requirements = academic_ele.html
        except Exception as e:
            err_list.append(f"academic_requirements — {e}")
        
        # ---------- 入学要求 ----------
        try:
            entry_require_general_desc_ele = page.ele('x://h3[contains(text(), "Admission to the Programme")]/following-sibling::ol[1]', timeout=5)
            if entry_require_general_desc_ele:
                entry_require_general_desc = entry_require_general_desc_ele.html
        except Exception as e:
            err_list.append(f"entry_require_general_desc — {e}")
# ---------- 完整入学要求（通用 + 中国） ----------
        try:
            
            entry_require = ""

            if entry_require_general_desc:
                entry_require += entry_require_general_desc


        except Exception as e:
            err_list.append(f"entry_require — {e}")
            entry_require = ""




        err_list = []

        # ---------- 原逻辑抓取 course_struct_desc ---------- #
        try:
            course_struct_desc = ""

            try:
                struct_ele = page.ele(
                    'x://h3[contains(., "Structure of the Programme")]/following-sibling::ol[1]',
                    timeout=5
                )
                if struct_ele:
                    course_struct_desc = struct_ele.html.strip()
            except:
                pass

            if not course_struct_desc:
                try:
                    html_list = page.run_js("""
                        let h3 = [...document.querySelectorAll('h3')]
                                .find(e => e.textContent.includes('Structure of the Programme'));
                        if (!h3) return [];
                        let res = [];
                        let n = h3.nextElementSibling;
                        while (n && n.tagName === 'P') {
                            res.push(n.outerHTML);
                            n = n.nextElementSibling;
                        }
                        return res;
                    """)
                    if html_list:
                        course_struct_desc = "\n".join(html_list)
                except:
                    pass

        except Exception as e:
            err_list.append(f"course_struct_desc fetch — {e}")


        from lxml import etree
        from DrissionPage import ChromiumPage
        
        import time
        programme_html = ""  # ⚠️ <-- 初始化，保证后面使用安全
        # ---------- programme_html 处理，去除2025部分 ---------- #
        try:
            # 获取包含课程结构的div元素
            programme_div = page.ele('x://div[@id="programme-structure"]')

            # 如果找到该div元素，提取HTML并删除2025部分
            if programme_div:
                programme_html = programme_div.html.strip()

                # 使用lxml解析HTML内容
                tree = etree.HTML(programme_html)

                # 查找包含 "2025" 的 <h3> 元素
                h3_2025 = tree.xpath('//h3[contains(text(), "2025")]')

                # 如果找到了包含2025的 <h3>，删除它及其后面的兄弟节点
                if h3_2025:
                    h3_2025 = h3_2025[0]  # 获取第一个包含 "2025" 的 <h3> 元素
                    parent = h3_2025.getparent()  # 获取父节点

                    # 获取 <h3>2025</h3> 之后的第一个兄弟节点
                    next_node = h3_2025.getnext()

                    # 循环删除2025部分及其后续兄弟节点，直到找到包含 "2026" 的 <h3>
                    while next_node is not None:
                        # 删除当前节点
                        parent.remove(next_node)

                        # 如果遇到包含 "2026" 的 <h3> 则停止删除
                        if next_node.tag == 'h3' and '2026' in next_node.text:
                            break

                        # 获取下一个兄弟节点
                        next_node = next_node.getnext()

                    # 删除 <h3>2025</h3> 节点本身
                    parent.remove(h3_2025)

                # 如果没有找到包含 "2025" 的 <h3>，则保留整个div，什么都不做
                else:
                    # 保留原 div 内容，无需删除
                    pass

                # 重新将处理后的HTML转换为字符串
                programme_html = etree.tostring(tree, pretty_print=True, encoding="unicode")

        except Exception as e:
            programme_html = ""
            print(f"错误: {e}")





        # ---------- 链接课程抓 <h1> 文本作为课程名称 ---------- #
        try:
            final_courses = []

            if course_struct_desc:
                links = page.run_js(f"""
                    let div = document.createElement('div');
                    div.innerHTML = `{course_struct_desc}`;
                    return [...div.querySelectorAll('a')].map(a => {{
                        return {{
                            text: a.textContent.trim(),
                            href: a.getAttribute('data-uw-original-href') || a.getAttribute('href')
                        }};
                    }});
                """)

                if links:
                    for item in links:
                        course_code = item["text"]
                        href = item["href"]

                        if href:
                            try:
                                # 进入子页面抓 <h1 class="page-banner__title"> 文本
                                sub_page = ChromiumPage()
                                sub_page.get(href, timeout=20)
                                ele = sub_page.ele('x://h1[@class="page-banner__title"]')
                                course_name = ele.text.strip() if ele else ""
                                if course_name:
                                    final_courses.append(f"{course_code}: {course_name}")
                                else:
                                    final_courses.append(course_code)
                            except:
                                final_courses.append(course_code)
                        else:
                            final_courses.append(course_code)

                    course_struct_desc = ", ".join(final_courses)

        except Exception as e:
            err_list.append(f"course_struct_desc — {e}")

        
        # ---------- 最终合并 program 内容，无论前面是否抓到结构 ---------- #
        try:
            if programme_html:
                course_struct_desc = f"{programme_html}\n{course_struct_desc}".strip()
        except Exception as e:
            err_list.append(f"programme merge — {e}")

        
        # ========== 学位 degree ==========
        try:
            h1_text = page.ele("x://h1[@class='page-banner__title']", timeout=10).text.strip()

            if "(" in h1_text and ")" in h1_text:
                first = h1_text.find("(")
                last = h1_text.rfind(")")
                degree = h1_text[first + 1:last].strip()
            else:
                degree = ""

        except Exception as e:
            err_list.append(f"学位获取失败——{e}")
            degree = ""
        
        # ========== 学院名称 faculty ==========
        try:
            html = page.html
            main_html = html.lower().split("academic divisions")[0]

            divisions = [
                "Division of Health Sciences",
                "Division of Humanities",
                "Division of Sciences",
                "Otago Business School",
            ]

            for d in divisions:
                if d.lower() in main_html:
                    faculty_name = d
                    break
        except Exception as e:
            faculty_name = ""
            err_list.append(f"学院名称获取失败——{e}")

        # ---------- 概述 ----------
        try:
            overview = ""

            # 1) 先找 id=overview 的 h2 或 div/section 容器
            title = page.ele('x://h2[@id="overview"]')

            if not title:
                # 如果没有 id=overview 的 h2，则找包含 Overview 的 h2（忽略大小写）
                title = page.ele(
                    'x://h2[contains(translate(.,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"overview")]'
                )

            if title:
                parts = []

                # 2) 从下一个兄弟节点开始
                sib = title.next()

                # 3) 连续抓 <p>，直到不是 p 停止
                while sib:
                    if sib.tag != "p":
                        break

                    html = sib.html
                    if html:
                        parts.append(html)

                    sib = sib.next()

                if parts:
                    overview = "\n".join(parts)



        except Exception as e:
            err_list.append(f"overview — {e}")
            overview = ""



        # ========== 学习方式 study_mode ==========
        try:
            html_lower = page.html.lower()

            if "full-time" in html_lower:
                study_mode = "full-time"
            elif "part-time" in html_lower:
                study_mode = "part-time"
            else:
                study_mode = ""
                err_list.append("study_mode")
        except Exception:
            study_mode = ""
            err_list.append("study_mode")

        # ---------- 获取 apply_url ----------
        try:
            apply_url = []  # 改为列表，用于存放多个链接
            
            # 第一步：直接点击Start application然后获取Continue链接
            try:
                btn = page.ele('x://button[contains(.,"Start application")]')
                if btn:
                    btn.click()
                    page.wait(1)

                a_ele = page.ele('x://a[contains(.,"Continue application")]')
                if a_ele:
                    href = a_ele.attr('href')
                    if href:
                        apply_url.append(href)
            except Exception as e:
                err_list.append(f"apply_url第一步 — {e}")
            
            # 第二步：如果第一步没找到，尝试三个地点的方法（收集所有找到的链接）
            if not apply_url:
                locations = ["Christchurch", "Dunedin", "Wellington"]
                
                for loc in locations:
                    try:
                        # 重新加载页面确保状态重置
                        page.get(major_url)
                        page.wait.doc_loaded()
                        page.wait(2)
                        
                        # 点击Start application按钮
                        btn = page.ele('x://button[contains(.,"Start application")]')
                        if btn:
                            btn.click()
                            page.wait(1)
                        
                        # 点击对应地点标签
                        if loc == "Christchurch":
                            ele = page.ele('x://h4[text()="Christchurch"]')
                        elif loc == "Dunedin":
                            ele = page.ele('x://h4[text()="Dunedin"]')
                        else:  # Wellington
                            ele = page.ele('x://h4[text()="Wellington"]')
                        
                        ele.parent().click()
                        page.wait(1)
                        
                        # 获取Continue链接
                        a_ele = page.ele('x://a[contains(.,"Continue application")]')
                        if a_ele:
                            href = a_ele.attr('href')
                            if href:
                                apply_url.append(href)  # 收集所有找到的链接，不break
                                
                    except Exception as e:
                        err_list.append(f"apply_url{loc}地点 — {e}")
                        continue  # 继续尝试下一个地点

        except Exception as e:
            err_list.append(f"apply_url — {e}")
            apply_url = []

        # ========== 学制 ==========
        try:
            expected_duration_ele = page.ele('x://dt[contains(.,"Duration")]/following-sibling::dd/span', timeout=5)
            if expected_duration_ele:
                expected_duration = expected_duration_ele.text
        except:
            expected_duration = ""

        # ---------- 学费 ----------

        try:
            # 先尝试第一种结构的学费信息（qualification-info-bar结构）
            fees_span = page.ele('x://span[contains(., "International fee 2026:")]')
            
            if fees_span:
                # 获取完整文本
                full_text = fees_span.text.strip()
                if "to be confirmed" not in full_text.lower():
                    # 提取学费金额
                    fee_match = re.search(r'International fee 2026:\s*([^<]+)', full_text)
                    if fee_match:
                        fee_text = fee_match.group(1).strip()
                        # 如果是数字格式，添加annual
                        if re.search(r'\d', fee_text):
                            fees = f"{fee_text} annual"
                        else:
                            fees = fee_text
                    else:
                        fees = full_text
                else:
                    fees = "To be confirmed"
            
            else:
                # 尝试第二种结构的学费信息（programme-details结构）
                fees_div = page.ele('x://div[@class="programme-details__fees-item" and contains(., "International 2026")]')
                
                if fees_div:
                    # 在找到的div中查找h3元素
                    fees_h3 = fees_div.ele('tag:h3', timeout=2)
                    if fees_h3:
                        fees_text = fees_h3.text.strip()
                        if fees_text and "to be confirmed" not in fees_text.lower():
                            fees = f"{fees_text} annual"
                        else:
                            fees = fees_text
                    else:
                        # 如果没有找到h3，尝试直接获取文本
                        fees = fees_div.text.split("International 2026")[-1].strip()
            
            # 如果以上都没找到，尝试其他可能的元素
            if 'fees' not in locals() or not fees or "to be confirmed" in str(fees).lower():
                # 尝试查找包含"NZ$"的学费信息
                fee_elements = page.eles('x://*[contains(text(), "NZ$") and (contains(text(), "International") or contains(text(), "2026"))]')
                if fee_elements:
                    for element in fee_elements:
                        fees_text = element.text.strip()
                        if fees_text and "to be confirmed" not in fees_text.lower():
                            fees = f"{fees_text} annual"
                            break
                
                # 最后尝试原始的路径
                if 'fees' not in locals() or not fees or "to be confirmed" in str(fees).lower():
                    try:
                        fees_ele = page.ele('x://div[contains(@class,"programme-details__fees-item")]//p[contains(., "International 2026")]/following-sibling::h3[1]')
                        if fees_ele:
                            fees_text = fees_ele.text.strip()
                            if fees_text and "to be confirmed" not in fees_text.lower():
                                fees = f"{fees_text} annual"
                            else:
                                fees = fees_text
                    except:
                        pass
            
            # 如果最终都没有找到学费信息，设为空字符串
            if 'fees' not in locals():
                fees = ""
            
        except Exception as e:
            err_list.append(f"fees — {e}")
            fees = ""
        # ---------- 英语语言要求 ----------
        try:
            language_require = ""

            # 1) 找到包含 English language requirements 的 h2 / h3 / h4
            title = page.ele(
                'x://h2[contains(translate(.,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"english language requirements")]'
                ' | //h3[contains(translate(.,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"english language requirements")]'
                ' | //h4[contains(translate(.,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"english language requirements")]'
            )

            if title:
                parts = []

                # 2) 从下一个兄弟节点开始
                sib = title.next()

                # 3) 连续收集 <p>，直到遇到不是 p
                while sib:
                    if sib.tag != "p":
                        break

                    html = sib.html
                    if html:
                        parts.append(html)

                    sib = sib.next()

                # 4) 拼成最终 HTML
                if parts:
                    language_require = "\n".join(parts)



        except Exception as e:
            err_list.append(f"language_require — {e}")
            language_require = ""

        final_major_json = {
            "major_url-href": major_url,
            "major_name": major_name,
            "degree": degree,
            "faculty_name": faculty_name,
            "overview": overview,
            "study_mode": study_mode,
            "expected_duration": expected_duration,
            "fees": fees,
            "language_require": language_require,
            "course_struct_desc": course_struct_desc,
            "academic_requirements": academic_requirements,
            "entry_require_general_desc": entry_require_general_desc,
            "entry_require": entry_require,
            "PTE": self.PTE,
            "TOEFL": self.TOEFL,
            "IELTS": self.IELTS,
            "application_start_date": self.application_start_date,
            "application_deadline": self.application_deadline,
            "start_date": self.start_date,

            "apply_url": apply_url,
        }

        # 存储数据到临时列表，供after_scrape处理
        self.crawled_majors.append(final_major_json)

        res = ScrapeResult(
            data=final_major_json,
            err_info={
                "url": major_url,
                "err_list": err_list
            }
        )

        return res
        





if __name__ == "__main__":
    spider = UniversityOfOtagoPgSpider()
    spider.run()
