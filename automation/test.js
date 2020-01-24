const puppeteer = require('puppeteer');

(async () => {
//  const browser = await puppeteer.launch();
    const browser = await puppeteer.launch({
        args: ['--proxy-server=zproxy.lum-superproxy.io:22225'],
        headless: true
    });
    const context = await browser.createIncognitoBrowserContext();
    const page = await browser.newPage();
    await page.authenticate({
        username:'lum-customer-blink_health-zone-apiscraper-country-us',
        password:'9l39ljqfuxzh'
    })
    await page.goto('https://www.goodrx.com/abilify',{waitUntil: 'networkidle0'});

//    await page.goto('https://whatismyipaddress.com/');
    await page.screenshot({path: 'example.png'});
    await context.close();
    await browser.close();
})();

