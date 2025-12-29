Excellent, you’ve done the right thing by inspecting the **actual HTML structure** of your Swagger UI!

Based on your findings:

* The **GET** method is marked by:

  ```html
  <span class='opblock-summary-method'>GET</span>
  ```
* The **Available Values** section is indicated by:

  ```html
  //i[text() = 'Available Values']
  ```

So, to reliably select all GET endpoints, **the best way is to select all `.opblock` containers which contain a `span` with text "GET" and class `opblock-summary-method`**.

---

## 🔥 **Here’s the Corrected Approach**

### 1. **Updated GET Endpoint XPath**

```python
GET_XPATH = '//div[contains(@class,"opblock")]//span[@class="opblock-summary-method" and text()="GET"]/ancestor::div[contains(@class,"opblock")]'
```

* This finds any `.opblock` div that has a descendant `span` with class "opblock-summary-method" and text "GET".

---

### 2. **Available Values XPath**

You can check for this under each GET endpoint:

```python
AVAILABLE_VALUES_XPATH = './/i[text()="Available Values"]/following-sibling::*'
```

Or, more flexibly, select the parent container that holds them (depends on your HTML).

---

## 🟢 **Plug It In: Script Snippet**

Replace in your code:

```python
GET_XPATH = '//div[contains(@class,"opblock")]//span[@class="opblock-summary-method" and text()="GET"]/ancestor::div[contains(@class,"opblock")]'
AVAILABLE_VALUES_XPATH = './/i[text()="Available Values"]/following-sibling::*'
```

---

## **Minimal Fix Example**

```python
GET_XPATH = '//div[contains(@class,"opblock")]//span[@class="opblock-summary-method" and text()="GET"]/ancestor::div[contains(@class,"opblock")]'
GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'
GET_EXPAND_BTN_XPATH = './/button[contains(@class,"opblock-summary-control")]'
AVAILABLE_VALUES_XPATH = './/i[text()="Available Values"]/following-sibling::*'
```

---

## **What this does:**

* Finds **all GET endpoints** robustly regardless of theme.
* Locates the **Available Values** section as per your real UI.
* All else in the code remains the same.

---

## **If you want to be super robust**

You can even select all endpoints and then **filter in Python** for those whose method span is `GET`, but the above XPath does it in one go.

---

## **Extra Debugging Tip**

If you want to see how many GET blocks were found and their names:

```python
for idx, get_block in enumerate(get_blocks):
    method = await get_block.locator('.//span[@class="opblock-summary-method"]').inner_text()
    summary = await get_block.locator(GET_SUMMARY_XPATH).inner_text()
    print(f"{idx+1}: {method} {summary}")
```

---

**Try these updated XPaths. You should see all your GET endpoints picked up!**

If not, paste a full HTML block of a GET endpoint and I’ll tune the XPath further for your UI.

---

**Let me know how this works for you!**
