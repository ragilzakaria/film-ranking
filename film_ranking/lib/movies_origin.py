from playwright.sync_api import sync_playwright
import re

pattern = r"Country of origin\n(.*)"


def get_country_of_origin(tconst: str):
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()

        # Open IMDb page for Spider-Man: Homecoming
        page.goto(f"https://www.imdb.com/title/{tconst}")

        # Wait until the string "IMDb" appears in the page content
        # page.wait_for_selector('text=IMDb')
        # Click on the "Country of origin" link
        details_element = page.locator('[data-testid="Details"]')
        text = details_element.inner_text()
        match = re.search(pattern, text)
        if match:
            country_of_origin = match.group(1)
            # Close the browser after the task is done
            browser.close()
            print(country_of_origin)
            return country_of_origin
        else:
            # Close the browser after the task is done
            browser.close()
            return None


if __name__ == "__main__":
    click_country_of_origin_link("tt1533056")
