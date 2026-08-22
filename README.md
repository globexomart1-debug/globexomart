# Globexomart Fresh Telegram Bot

This deployment uses MongoDB database `globexomart_fresh_v1`. It does not read the old bot database.

## Railway variables
Set:
- `BOT_TOKEN`
- `ADMIN_ID`
- `MONGO_URI`
- optional `BOT_WORKERS=12`

## Deploy
Push this folder to GitHub, create a Railway service from the repository, set the variables, and deploy.

For force-join and timed VIP-channel removal, add the bot as admin in those chats with invite/member-ban permissions.

## User menu
Methods, Services, Buy VIP, Points, Referral, Account, Chat ID, Redeem, Deposit, Withdraw.

## Rules
- Free Methods: free or points.
- VIP Methods: included for VIP; free users can buy individual methods in USDT.
- Free Service: free or points.
- Paid Service: USDT purchase for everyone, including VIP.
- Patched methods show 🛑 but remain accessible.
- VIP expiry reminders start 5 days before expiry and repeat daily.
- Expired VIP is removed from configured VIP channels/groups.

## Default VIP plans
1M $25, 2M $40, 4M $60, 1Y $100.

## Automatic payments
Manual screenshot approval is included. The code retains `process_verified_auto_payment(...)` as the activation hook for a verified payment-provider integration. Do not call it until a trusted API/provider independently verifies the payment.

## Latest service shop & payment flow
- Services now work as digital-product listings with: name, price, duration, warranty, and IN STOCK / OUT OF STOCK status.
- Admin uploads Free Service or Paid Service from the admin panel and enters the product metadata during upload.
- Product Manager can toggle stock and edit duration/warranty with commands shown in the panel.
- Paid services require USDT for every user, including VIP members.
- Paid-service purchase proof requires both transaction ID and screenshot before the admin receives an approval request.
- VIP manual payment proof also requires transaction ID + screenshot before admin review.
- Admin review shows username (when available), Telegram user ID, chat ID, plan/item, amount, TxID, and screenshot.
- VIP activates automatically after admin approval for the selected plan interval.
- When VIP expires, bot VIP status is cleared and the user is removed from configured VIP access chats automatically.
