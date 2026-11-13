"""ONIX 3.0 参考名（reference names）XML 子集的解析、投影与订正原语。

固定支持的子集（release 3.0，按本地名匹配，命名空间前缀不影响匹配）：

- 标识 isbn: ``ProductIdentifier`` 且 ``ProductIDType`` 为 ``15``（ISBN-13）或
  ``03``（GTIN-13）时的 ``IDValue``；
- 标题 title: ``TitleDetail[TitleType=01]/TitleElement[TitleElementLevel=01]/TitleText``；
- 出版者 publisher: ``PublishingDetail/Publisher[PublishingRole=01]/PublisherName``；
- 版次 edition: ``DescriptiveDetail/EditionNumber``（缺失时不默认为 1，不凑主键）；
- 语言 language: ``DescriptiveDetail/Language[LanguageRole=01]/LanguageCode``；
- 供货 availability: ``ProductSupply/SupplyDetail/ProductAvailability``（原始代码值）。

同一字段命中多个同角色节点时不拼接、不默认取第一项：必需字段（isbn/title/
publisher/edition）判为不可投影并给出原因；可选字段（language/availability）
留空并记录候选位置。其余元素、重复结构、属性、注释与处理指令在 DOM 中原样
保留，导出时按语义重建（不要求字节级相同）。
"""

from __future__ import annotations

import hashlib
from xml.parsers import expat
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Callable, Iterator


ISBN_ID_TYPES = ("15", "03")
ROLE_EXPECTED = {"title": "01", "publisher": "01", "language": "01"}
REQUIRED_FIELDS = ("isbn", "title", "publisher", "edition")
OPTIONAL_FIELDS = ("language", "availability")
CORRECTABLE_FIELDS = ("title", "publisher", "language")
LEAF_FOR_FIELD = {
    "title": "TitleText",
    "publisher": "PublisherName",
    "language": "LanguageCode",
}


@dataclass
class Comment:
    text: str


@dataclass
class ProcessingInstruction:
    target: str
    data: str


@dataclass
class DoctypeDecl:
    text: str


@dataclass
class Element:
    tag: str
    attributes: dict[str, str]
    children: list[Any]
    line: int
    column: int

    @property
    def local(self) -> str:
        return self.tag.rsplit(":", 1)[-1]


@dataclass
class Document:
    declaration: str
    prolog: list[Any]
    root: Element
    epilog: list[Any]


def parse(xml_text: str) -> Document:
    """把 ONIX XML 文本解析为保真 DOM；格式错误时抛 ValueError。"""
    parser = expat.ParserCreate()
    parser.buffer_text = True
    stack: list[Element] = []
    prolog: list[Any] = []
    epilog: list[Any] = []
    holder = {"root": None, "declaration": ""}

    def start(tag: str, attrs: dict[str, str]) -> None:
        el = Element(tag, dict(attrs), [], parser.CurrentLineNumber, parser.CurrentColumnNumber)
        if stack:
            stack[-1].children.append(el)
        elif holder["root"] is None:
            holder["root"] = el
        else:
            raise ValueError("document has multiple root elements")
        stack.append(el)

    def end(_tag: str) -> None:
        stack.pop()

    def text(data: str) -> None:
        if stack:
            stack[-1].children.append(data)

    def misc(node: Any) -> None:
        if stack:
            stack[-1].children.append(node)
        elif holder["root"] is None:
            prolog.append(node)
        else:
            epilog.append(node)

    def xml_decl(version: str, encoding: str | None, standalone: int) -> None:
        decl = f'<?xml version="{version}"'
        if encoding:
            decl += f' encoding="{encoding}"'
        if standalone == 1:
            decl += ' standalone="yes"'
        elif standalone == 0:
            decl += ' standalone="no"'
        holder["declaration"] = decl + "?>"

    def doctype(name: str, sysid: str | None, pubid: str | None, has_internal_subset: int) -> None:
        if has_internal_subset:
            raise ValueError("DOCTYPE with an internal subset is not supported")
        text = f"<!DOCTYPE {name}"
        if pubid:
            text += f' PUBLIC "{pubid}" "{sysid}"'
        elif sysid:
            text += f' SYSTEM "{sysid}"'
        prolog.append(DoctypeDecl(text + ">"))

    parser.StartElementHandler = start
    parser.EndElementHandler = end
    parser.CharacterDataHandler = text
    parser.CommentHandler = lambda data: misc(Comment(data))
    parser.ProcessingInstructionHandler = lambda target, data: misc(ProcessingInstruction(target, data))
    parser.XmlDeclHandler = xml_decl
    parser.StartDoctypeDeclHandler = doctype

    try:
        parser.Parse(xml_text, True)
    except expat.ExpatError as exc:
        raise ValueError(f"not well-formed ONIX XML: {exc}") from exc

    if holder["root"] is None:
        raise ValueError("document has no root element")
    return Document(holder["declaration"], prolog, holder["root"], epilog)


def _escape_text(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_attr(value: str) -> str:
    return _escape_text(value).replace('"', "&quot;")


def _serialize_node(node: Any) -> str:
    if isinstance(node, Element):
        attrs = "".join(
            f' {name}="{_escape_attr(value)}"' for name, value in node.attributes.items()
        )
        if not node.children:
            return f"<{node.tag}{attrs}/>"
        inner = "".join(_serialize_node(child) for child in node.children)
        return f"<{node.tag}{attrs}>{inner}</{node.tag}>"
    if isinstance(node, Comment):
        return f"<!--{node.text}-->"
    if isinstance(node, ProcessingInstruction):
        if node.data:
            return f"<?{node.target} {node.data}?>"
        return f"<?{node.target}?>"
    if isinstance(node, DoctypeDecl):
        return node.text
    return _escape_text(node)


def serialize(doc: Document) -> str:
    """把 DOM 重建为 XML 文本；未修改的子树保持原有字符数据。"""
    parts: list[str] = []
    if doc.declaration:
        parts.append(doc.declaration)
    parts.extend(_serialize_node(node) for node in doc.prolog)
    parts.append(_serialize_node(doc.root))
    parts.extend(_serialize_node(node) for node in doc.epilog)
    return "\n".join(parts) + "\n"


def fingerprint(xml_text: str) -> str:
    """消息版本指纹：订正提交与冲突检测的基准。"""
    return hashlib.sha256(xml_text.encode("utf-8")).hexdigest()


def child_elements(el: Element, local: str | None = None) -> list[Element]:
    found = [c for c in el.children if isinstance(c, Element)]
    if local is not None:
        found = [c for c in found if c.local == local]
    return found


def first_child(el: Element, local: str) -> Element | None:
    for child in child_elements(el):
        if child.local == local:
            return child
    return None


def element_text(el: Element) -> str:
    return "".join(c for c in el.children if isinstance(c, str)).strip()


def child_text(el: Element, local: str) -> str | None:
    child = first_child(el, local)
    if child is None:
        return None
    return element_text(child)


def iter_indexed(el: Element) -> Iterator[tuple[Element, int]]:
    """按同类兄弟节点中的 1 基序号产出子元素。"""
    counts: dict[str, int] = {}
    for child in child_elements(el):
        counts[child.local] = counts.get(child.local, 0) + 1
        yield child, counts[child.local]


def join_path(parent_path: str, local: str, index: int) -> str:
    return f"{parent_path}/{local}[{index}]"


def find_by_path(root: Element, path: str) -> Element | None:
    segments = [s for s in path.split("/") if s]
    if not segments:
        return None
    node = root
    for depth, segment in enumerate(segments):
        local, _, rest = segment.rpartition("[")
        if local and rest.endswith("]") and rest[:-1].isdigit():
            index = int(rest[:-1])
        else:
            local, index = segment, 1
        if depth == 0:
            if node.local != local or index != 1:
                return None
            continue
        matches = child_elements(node, local)
        if len(matches) < index:
            return None
        node = matches[index - 1]
    return node


def iter_products(doc: Document) -> Iterator[tuple[Element, str, int]]:
    """产出 (Product 元素, 元素路径, 产品序号)。"""
    root = doc.root
    if root.local != "ONIXMessage":
        raise ValueError(f"root element must be ONIXMessage, got {root.local}")
    order = 0
    for el, index in iter_indexed(root):
        if el.local == "Product":
            order += 1
            path = f"/{root.local}/Product[{index}]"
            yield el, path, order


def header_info(doc: Document) -> dict[str, str | None]:
    header = first_child(doc.root, "Header")
    sender = sent_at = None
    if header is not None:
        sender_el = first_child(header, "Sender")
        if sender_el is not None:
            sender = child_text(sender_el, "SenderName")
        sent_at = child_text(header, "SentDateTime")
    return {"sender": sender, "sent_at": sent_at}


@dataclass
class Candidate:
    path: str
    line: int
    role: str | None
    value: str

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "line": self.line, "role": self.role, "value": self.value}


def _isbn_candidates(product: Element, path: str) -> list[Candidate]:
    found = []
    for ident, i in iter_indexed(product):
        if ident.local != "ProductIdentifier":
            continue
        id_type = child_text(ident, "ProductIDType")
        if id_type not in ISBN_ID_TYPES:
            continue
        ident_path = join_path(path, "ProductIdentifier", i)
        for value_el, j in iter_indexed(ident):
            if value_el.local == "IDValue":
                found.append(
                    Candidate(join_path(ident_path, "IDValue", j), value_el.line, id_type,
                              element_text(value_el))
                )
    return found


def _descriptive_detail(product: Element, path: str) -> Iterator[tuple[Element, str]]:
    for dd, i in iter_indexed(product):
        if dd.local == "DescriptiveDetail":
            yield dd, join_path(path, "DescriptiveDetail", i)


def _title_candidates(product: Element, path: str) -> list[Candidate]:
    found = []
    for dd, dd_path in _descriptive_detail(product, path):
        for detail, i in iter_indexed(dd):
            if detail.local != "TitleDetail":
                continue
            title_type = child_text(detail, "TitleType")
            detail_path = join_path(dd_path, "TitleDetail", i)
            for element, j in iter_indexed(detail):
                if element.local != "TitleElement" or child_text(element, "TitleElementLevel") != "01":
                    continue
                element_path = join_path(detail_path, "TitleElement", j)
                for text_el, k in iter_indexed(element):
                    if text_el.local == "TitleText":
                        found.append(
                            Candidate(join_path(element_path, "TitleText", k), text_el.line,
                                      title_type, element_text(text_el))
                        )
    return found


def _publisher_candidates(product: Element, path: str) -> list[Candidate]:
    found = []
    for pd, i in iter_indexed(product):
        if pd.local != "PublishingDetail":
            continue
        pd_path = join_path(path, "PublishingDetail", i)
        for publisher, j in iter_indexed(pd):
            if publisher.local != "Publisher":
                continue
            role = child_text(publisher, "PublishingRole")
            publisher_path = join_path(pd_path, "Publisher", j)
            for name_el, k in iter_indexed(publisher):
                if name_el.local == "PublisherName":
                    found.append(
                        Candidate(join_path(publisher_path, "PublisherName", k), name_el.line,
                                  role, element_text(name_el))
                    )
    return found


def _edition_candidates(product: Element, path: str) -> list[Candidate]:
    found = []
    for dd, dd_path in _descriptive_detail(product, path):
        for edition, i in iter_indexed(dd):
            if edition.local == "EditionNumber":
                found.append(
                    Candidate(join_path(dd_path, "EditionNumber", i), edition.line, None,
                              element_text(edition))
                )
    return found


def _language_candidates(product: Element, path: str) -> list[Candidate]:
    found = []
    for dd, dd_path in _descriptive_detail(product, path):
        for language, i in iter_indexed(dd):
            if language.local != "Language":
                continue
            role = child_text(language, "LanguageRole")
            language_path = join_path(dd_path, "Language", i)
            for code_el, j in iter_indexed(language):
                if code_el.local == "LanguageCode":
                    found.append(
                        Candidate(join_path(language_path, "LanguageCode", j), code_el.line,
                                  role, element_text(code_el))
                    )
    return found


def _availability_candidates(product: Element, path: str) -> list[Candidate]:
    found = []
    for supply, i in iter_indexed(product):
        if supply.local != "ProductSupply":
            continue
        supply_path = join_path(path, "ProductSupply", i)
        for detail, j in iter_indexed(supply):
            if detail.local != "SupplyDetail":
                continue
            detail_path = join_path(supply_path, "SupplyDetail", j)
            for avail, k in iter_indexed(detail):
                if avail.local == "ProductAvailability":
                    found.append(
                        Candidate(join_path(detail_path, "ProductAvailability", k), avail.line,
                                  None, element_text(avail))
                    )
    return found


RESOLVERS: dict[str, Callable[[Element, str], list[Candidate]]] = {
    "isbn": _isbn_candidates,
    "title": _title_candidates,
    "publisher": _publisher_candidates,
    "edition": _edition_candidates,
    "language": _language_candidates,
    "availability": _availability_candidates,
}


@dataclass
class FieldProjection:
    field: str
    status: str
    value: str | None
    node_path: str | None
    node_line: int | None
    candidates: list[Candidate] = dataclass_field(default_factory=list)


@dataclass
class ProductProjection:
    product_index: int
    product_path: str
    record_reference: str | None
    fields: dict[str, FieldProjection]
    reasons: list[str]
    issues: list[str]

    @property
    def ok(self) -> bool:
        return not self.reasons

    def payload(self) -> dict[str, Any]:
        return {name: (fp.value if fp.status == "unique" else None)
                for name, fp in self.fields.items()}


def project_product(product: Element, path: str, index: int) -> ProductProjection:
    fields: dict[str, FieldProjection] = {}
    matched_counts: dict[str, int] = {}
    for name, resolver in RESOLVERS.items():
        candidates = resolver(product, path)
        expected = ROLE_EXPECTED.get(name)
        if expected is not None:
            matched = [c for c in candidates if c.role == expected]
        else:
            matched = candidates
        matched_counts[name] = len(matched)
        if len(matched) == 1:
            only = matched[0]
            fields[name] = FieldProjection(name, "unique", only.value, only.path, only.line,
                                           candidates)
        elif not matched:
            fields[name] = FieldProjection(name, "missing", None, None, None, candidates)
        else:
            fields[name] = FieldProjection(name, "ambiguous", None, None, None, candidates)

    reasons: list[str] = []
    for name in REQUIRED_FIELDS:
        projection = fields[name]
        if projection.status == "missing":
            reasons.append(_missing_reason(name))
        elif projection.status == "ambiguous":
            reasons.append(_ambiguous_reason(name, matched_counts[name]))

    issues: list[str] = []
    for name in OPTIONAL_FIELDS:
        if fields[name].status == "ambiguous":
            issues.append(_ambiguous_reason(name, matched_counts[name]) + "; left empty")

    return ProductProjection(index, path, child_text(product, "RecordReference"), fields,
                             reasons, issues)


def _missing_reason(name: str) -> str:
    return {
        "isbn": "isbn: missing (no ProductIdentifier with ProductIDType 15/03)",
        "title": "title: missing (no product-level TitleText with TitleType 01)",
        "publisher": "publisher: missing (no PublisherName with PublishingRole 01)",
        "edition": "edition: missing (EditionNumber absent; not defaulted to 1)",
    }[name]


def _ambiguous_reason(name: str, count: int) -> str:
    return {
        "isbn": f"isbn: ambiguous ({count} ProductIdentifier candidates with ProductIDType 15/03)",
        "title": f"title: ambiguous ({count} TitleText candidates with TitleType 01)",
        "publisher": f"publisher: ambiguous ({count} PublisherName candidates with PublishingRole 01)",
        "edition": f"edition: ambiguous ({count} EditionNumber elements)",
        "language": f"language: ambiguous ({count} LanguageCode candidates with LanguageRole 01)",
        "availability": f"availability: ambiguous ({count} ProductAvailability elements)",
    }[name]


def project_message(doc: Document) -> list[ProductProjection]:
    return [project_product(el, path, index) for el, path, index in iter_products(doc)]


def locate_product(
    doc: Document,
    record_reference: str | None = None,
    product_path: str | None = None,
) -> tuple[Element, str, int]:
    products = list(iter_products(doc))
    if product_path:
        for el, path, index in products:
            if path == product_path:
                return el, path, index
        raise ValueError(f"no Product element at path {product_path}")
    if record_reference:
        matches = [
            (el, path, index)
            for el, path, index in products
            if child_text(el, "RecordReference") == record_reference
        ]
        if not matches:
            raise ValueError(f"no product with RecordReference {record_reference!r}")
        if len(matches) > 1:
            paths = [path for _, path, _ in matches]
            raise ValueError(
                f"RecordReference {record_reference!r} matches {len(matches)} products: {paths}"
            )
        return matches[0]
    raise ValueError("a product selector (record_reference or product_path) is required")


def correction_candidates(product: Element, product_path: str, field: str) -> list[Candidate]:
    if field not in CORRECTABLE_FIELDS:
        raise ValueError(
            f"field must be one of {CORRECTABLE_FIELDS}, got {field!r}"
        )
    return RESOLVERS[field](product, product_path)


def choose_candidate(
    candidates: list[Candidate], field: str, node_path: str | None = None
) -> Candidate | None:
    """返回唯一目标节点；角色有歧义时返回 None（由调用方暴露候选位置）。

    显式给定 node_path 时，该路径必须是候选之一，否则抛 ValueError——
    订正只能落在可定位的字段节点上，不能改写任意节点。
    """
    if node_path:
        for candidate in candidates:
            if candidate.path == node_path:
                return candidate
        raise ValueError(f"node path {node_path} is not a {field} candidate")
    expected = ROLE_EXPECTED[field]
    matched = [c for c in candidates if c.role == expected]
    if len(matched) == 1:
        return matched[0]
    return None


def apply_text_correction(doc: Document, candidate: Candidate, new_value: str) -> None:
    node = find_by_path(doc.root, candidate.path)
    if node is None:
        raise ValueError(f"candidate node no longer exists: {candidate.path}")
    if any(isinstance(child, Element) for child in node.children):
        raise ValueError(f"target node {candidate.path} has child elements; refusing to rewrite")
    node.children = [new_value]
